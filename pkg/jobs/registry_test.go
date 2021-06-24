// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobs

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeColumnMutation adds column as a mutation and writes the
// descriptor to the DB.
func writeColumnMutation(
	t *testing.T,
	kvDB *kv.DB,
	tableDesc *tabledesc.Mutable,
	column string,
	m descpb.DescriptorMutation,
) {
	col, err := tableDesc.FindColumnWithName(tree.Name(column))
	if err != nil {
		t.Fatal(err)
	}
	for i := range tableDesc.Columns {
		if col.GetID() == tableDesc.Columns[i].ID {
			// Use [:i:i] to prevent reuse of existing slice, or outstanding refs
			// to ColumnDescriptors may unexpectedly change.
			tableDesc.Columns = append(tableDesc.Columns[:i:i], tableDesc.Columns[i+1:]...)
			break
		}
	}
	m.Descriptor_ = &descpb.DescriptorMutation_Column{Column: col.ColumnDesc()}
	writeMutation(t, kvDB, tableDesc, m)
}

// writeMutation writes the mutation to the table descriptor.
func writeMutation(
	t *testing.T, kvDB *kv.DB, tableDesc *tabledesc.Mutable, m descpb.DescriptorMutation,
) {
	tableDesc.Mutations = append(tableDesc.Mutations, m)
	tableDesc.Version++
	if err := catalog.ValidateSelf(tableDesc); err != nil {
		t.Fatal(err)
	}
	if err := kvDB.Put(
		context.Background(),
		catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, tableDesc.ID),
		tableDesc.DescriptorProto(),
	); err != nil {
		t.Fatal(err)
	}
}

func writeGCMutation(
	t *testing.T,
	kvDB *kv.DB,
	tableDesc *tabledesc.Mutable,
	m descpb.TableDescriptor_GCDescriptorMutation,
) {
	tableDesc.GCMutations = append(tableDesc.GCMutations, m)
	tableDesc.Version++
	if err := catalog.ValidateSelf(tableDesc); err != nil {
		t.Fatal(err)
	}
	if err := kvDB.Put(
		context.Background(),
		catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, tableDesc.GetID()),
		tableDesc.DescriptorProto(),
	); err != nil {
		t.Fatal(err)
	}
}

type mutationOptions struct {
	// Set if the desc should have any mutations of any sort.
	hasMutation bool
	// Set if the mutation being inserted is a GCMutation.
	hasGCMutation bool
	// Set if the desc should have a job that is dropping it.
	hasDropJob bool
}

func (m mutationOptions) string() string {
	return fmt.Sprintf("hasMutation=%s_hasGCMutation=%s_hasDropJob=%s",
		strconv.FormatBool(m.hasMutation), strconv.FormatBool(m.hasGCMutation),
		strconv.FormatBool(m.hasDropJob))
}

func TestRegistryGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	db := sqlutils.MakeSQLRunner(sqlDB)

	ts := timeutil.Now()
	earlier := ts.Add(-1 * time.Hour)
	muchEarlier := ts.Add(-2 * time.Hour)

	setDropJob := func(dbName, tableName string) {
		desc := catalogkv.TestingGetMutableExistingTableDescriptor(
			kvDB, keys.SystemSQLCodec, dbName, tableName)
		desc.DropJobID = 123
		if err := kvDB.Put(
			context.Background(),
			catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, desc.GetID()),
			desc.DescriptorProto(),
		); err != nil {
			t.Fatal(err)
		}
	}

	constructTableName := func(prefix string, mutOptions mutationOptions) string {
		return fmt.Sprintf("%s_%s", prefix, mutOptions.string())
	}

	writeJob := func(name string, created, finished time.Time, status Status, mutOptions mutationOptions) string {
		tableName := constructTableName(name, mutOptions)
		if _, err := sqlDB.Exec(fmt.Sprintf(`
CREATE DATABASE IF NOT EXISTS t; 
CREATE TABLE t."%s" (k VARCHAR PRIMARY KEY DEFAULT 'default', v VARCHAR,i VARCHAR NOT NULL DEFAULT 'i');
INSERT INTO t."%s" VALUES('a', 'foo');
`, tableName, tableName)); err != nil {
			t.Fatal(err)
		}
		tableDesc := catalogkv.TestingGetMutableExistingTableDescriptor(
			kvDB, keys.SystemSQLCodec, "t", tableName)
		if mutOptions.hasDropJob {
			setDropJob("t", tableName)
		}
		if mutOptions.hasMutation {
			writeColumnMutation(t, kvDB, tableDesc, "i", descpb.DescriptorMutation{State: descpb.
				DescriptorMutation_DELETE_AND_WRITE_ONLY, Direction: descpb.DescriptorMutation_DROP})
		}
		if mutOptions.hasGCMutation {
			writeGCMutation(t, kvDB, tableDesc, descpb.TableDescriptor_GCDescriptorMutation{})
		}

		payload, err := protoutil.Marshal(&jobspb.Payload{
			Description: name,
			// register a mutation on the table so that jobs that reference
			// the table are not considered orphaned
			DescriptorIDs: []descpb.ID{
				tableDesc.GetID(),
				descpb.InvalidID, // invalid id to test handling of missing descriptors.
			},
			Details:        jobspb.WrapPayloadDetails(jobspb.SchemaChangeDetails{}),
			StartedMicros:  timeutil.ToUnixMicros(created),
			FinishedMicros: timeutil.ToUnixMicros(finished),
		})
		if err != nil {
			t.Fatal(err)
		}
		progress, err := protoutil.Marshal(&jobspb.Progress{
			Details: jobspb.WrapProgressDetails(jobspb.SchemaChangeProgress{}),
		})
		if err != nil {
			t.Fatal(err)
		}

		var id jobspb.JobID
		db.QueryRow(t,
			`INSERT INTO system.jobs (status, payload, progress, created) VALUES ($1, $2, $3, $4) RETURNING id`,
			status, payload, progress, created).Scan(&id)
		return strconv.Itoa(int(id))
	}

	// Test the descriptor when any of the following are set.
	// 1. Mutations
	// 2. GC Mutations
	// 3. A drop job
	for _, hasMutation := range []bool{true, false} {
		for _, hasGCMutation := range []bool{true, false} {
			for _, hasDropJob := range []bool{true, false} {
				if !hasMutation && !hasGCMutation && !hasDropJob {
					continue
				}
				mutOptions := mutationOptions{
					hasMutation:   hasMutation,
					hasGCMutation: hasGCMutation,
					hasDropJob:    hasDropJob,
				}
				oldRunningJob := writeJob("old_running", muchEarlier, time.Time{}, StatusRunning, mutOptions)
				oldSucceededJob := writeJob("old_succeeded", muchEarlier, muchEarlier.Add(time.Minute), StatusSucceeded, mutOptions)
				oldFailedJob := writeJob("old_failed", muchEarlier, muchEarlier.Add(time.Minute),
					StatusFailed, mutOptions)
				oldRevertFailedJob := writeJob("old_revert_failed", muchEarlier, muchEarlier.Add(time.Minute),
					StatusRevertFailed, mutOptions)
				oldCanceledJob := writeJob("old_canceled", muchEarlier, muchEarlier.Add(time.Minute),
					StatusCanceled, mutOptions)
				newRunningJob := writeJob("new_running", earlier, earlier.Add(time.Minute), StatusRunning,
					mutOptions)
				newSucceededJob := writeJob("new_succeeded", earlier, earlier.Add(time.Minute), StatusSucceeded, mutOptions)
				newFailedJob := writeJob("new_failed", earlier, earlier.Add(time.Minute), StatusFailed, mutOptions)
				newRevertFailedJob := writeJob("new_revert_failed", earlier, earlier.Add(time.Minute), StatusRevertFailed, mutOptions)
				newCanceledJob := writeJob("new_canceled", earlier, earlier.Add(time.Minute),
					StatusCanceled, mutOptions)

				db.CheckQueryResults(t, `SELECT id FROM system.jobs ORDER BY id`, [][]string{
					{oldRunningJob}, {oldSucceededJob}, {oldFailedJob}, {oldRevertFailedJob}, {oldCanceledJob},
					{newRunningJob}, {newSucceededJob}, {newFailedJob}, {newRevertFailedJob}, {newCanceledJob}})

				if err := s.JobRegistry().(*Registry).cleanupOldJobs(ctx, earlier); err != nil {
					t.Fatal(err)
				}
				db.CheckQueryResults(t, `SELECT id FROM system.jobs ORDER BY id`, [][]string{
					{oldRunningJob}, {oldRevertFailedJob}, {newRunningJob}, {newSucceededJob},
					{newFailedJob}, {newRevertFailedJob}, {newCanceledJob}})

				if err := s.JobRegistry().(*Registry).cleanupOldJobs(ctx, ts.Add(time.Minute*-10)); err != nil {
					t.Fatal(err)
				}
				db.CheckQueryResults(t, `SELECT id FROM system.jobs ORDER BY id`, [][]string{
					{oldRunningJob}, {oldRevertFailedJob}, {newRunningJob}, {newRevertFailedJob}})

				// Delete the revert failed, and running jobs for the next run of the
				// test.
				_, err := sqlDB.Exec(`DELETE FROM system.jobs WHERE id = $1 OR id = $2 OR id = $3 OR id = $4`,
					oldRevertFailedJob, newRevertFailedJob, oldRunningJob, newRunningJob)
				require.NoError(t, err)
			}
		}
	}
}

func TestRegistryGCPagination(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	db := sqlutils.MakeSQLRunner(sqlDB)
	defer s.Stopper().Stop(ctx)

	for i := 0; i < 2*cleanupPageSize+1; i++ {
		payload, err := protoutil.Marshal(&jobspb.Payload{})
		require.NoError(t, err)
		db.Exec(t,
			`INSERT INTO system.jobs (status, created, payload) VALUES ($1, $2, $3)`,
			StatusCanceled, timeutil.Now().Add(-time.Hour), payload)
	}

	ts := timeutil.Now()
	require.NoError(t, s.JobRegistry().(*Registry).cleanupOldJobs(ctx, ts.Add(-10*time.Minute)))
	var count int
	db.QueryRow(t, `SELECT count(1) FROM system.jobs`).Scan(&count)
	require.Zero(t, count)
}

func TestBatchJobsCreation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, test := range []struct {
		name      string
		batchSize int
	}{
		{"small batch", 10},
		{"medium batch", 501},
		{"large batch", 1001},
		{"extra large batch", 5001},
	} {
		t.Run(test.name, func(t *testing.T) {
			{
				if test.batchSize > 10 {
					skip.UnderStress(t, "skipping stress test for batch size ", test.batchSize)
					skip.UnderRace(t, "skipping test for batch size ", test.batchSize)
				}

				args := base.TestServerArgs{
					Knobs: base.TestingKnobs{
						JobsTestingKnobs: NewTestingKnobsWithShortIntervals(),
					},
				}

				ctx := context.Background()
				s, sqlDB, kvDB := serverutils.StartServer(t, args)
				tdb := sqlutils.MakeSQLRunner(sqlDB)
				defer s.Stopper().Stop(ctx)
				r := s.JobRegistry().(*Registry)

				RegisterConstructor(jobspb.TypeImport, func(job *Job, cs *cluster.Settings) Resumer {
					return FakeResumer{
						OnResume: func(ctx context.Context) error {
							return nil
						},
					}
				})

				// Create a batch of job specifications.
				var records []*Record
				for i := 0; i < test.batchSize; i++ {
					records = append(records, &Record{
						JobID:    r.MakeJobID(),
						Details:  jobspb.ImportDetails{},
						Progress: jobspb.ImportProgress{},
					})
				}
				// Create jobs in a batch.
				var jobIDs []jobspb.JobID
				require.NoError(t, kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
					var err error
					jobIDs, err = r.CreateJobsWithTxn(ctx, txn, records)
					return err
				}))
				require.Equal(t, len(jobIDs), test.batchSize)
				// Wait for the jobs to complete.
				tdb.CheckQueryResultsRetry(t, "SELECT count(*) FROM [SHOW JOBS]",
					[][]string{{fmt.Sprintf("%d", test.batchSize)}})
				for _, id := range jobIDs {
					tdb.CheckQueryResultsRetry(t, fmt.Sprintf("SELECT status FROM system.jobs WHERE id = '%d'", id),
						[][]string{{"succeeded"}})
				}
			}
		})
	}
}

// TestRetriesWithExponentialBackoff tests the working of exponential delays
// when jobs are retried. Moreover, it tests the effectiveness of the upper
// bound on the retry delay.
func TestRetriesWithExponentialBackoff(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	unitTime := time.Millisecond
	clusterSettings := func(
		ctx context.Context, initialDelay time.Duration, maxDelay time.Duration,
	) *cluster.Settings {
		s := cluster.MakeTestingClusterSettings()
		// Set a small adopt and cancel intervals to reduce test time.
		adoptIntervalSetting.Override(ctx, &s.SV, unitTime)
		cancelIntervalSetting.Override(ctx, &s.SV, unitTime)
		retryInitialDelaySetting.Override(ctx, &s.SV, initialDelay)
		retryMaxDelaySetting.Override(ctx, &s.SV, maxDelay)
		return s
	}

	// createJob creates a fake job that we control for testing.
	createJob := func(
		ctx context.Context, s serverutils.TestServerInterface, r *Registry, kvDB *kv.DB,
	) jobspb.JobID {
		id := r.MakeJobID()
		require.NoError(t, kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			_, err := r.CreateJobWithTxn(ctx, Record{
				Details:  jobspb.ImportDetails{},
				Progress: jobspb.ImportProgress{},
			}, id, txn)
			return err
		}))
		return id
	}
	validateCounts := func(t *testing.T, expectedResumed, resumed int64) {
		require.Equal(t, expectedResumed, resumed, "unexpected number of jobs resumed")
	}
	waitUntilCount := func(t *testing.T, counter *metric.Counter, count int64) {
		testutils.SucceedsSoon(t, func() error {
			cnt := counter.Count()
			if cnt >= count {
				return nil
			}
			return errors.Errorf(
				"waiting for %v to reach %d, currently at %d", counter.GetName(), count, cnt,
			)
		})
	}
	waitUntilStatus := func(t *testing.T, tdb *sqlutils.SQLRunner, jobID jobspb.JobID, status Status) {
		tdb.CheckQueryResultsRetry(t,
			fmt.Sprintf("SELECT status FROM [SHOW JOBS] WHERE job_id = %d", jobID),
			[][]string{{string(status)}})
	}

	// pauseOrCancelJob pauses or cancels a job. If pauseJob is true, the job is paused,
	// otherwise the job is canceled.
	pauseOrCancelJob := func(
		t *testing.T, ctx context.Context, db *kv.DB, registry *Registry, jobID jobspb.JobID, pauseJob bool,
	) {
		assert.NoError(t, db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			if pauseJob {
				return registry.PauseRequested(ctx, txn, jobID)
			}
			return registry.CancelRequested(ctx, txn, jobID)
		}))
	}

	// nextDelay returns the next delay based calculated from the given retryCnt
	// and exponential-backoff parameters.
	nextDelay := func(
		retryCnt int, initDelay time.Duration, maxDelay time.Duration,
	) time.Duration {
		delay := initDelay * ((1 << int(math.Min(62, float64(retryCnt)))) - 1)
		if delay < 0 {
			delay = maxDelay
		}
		return time.Duration(math.Min(float64(delay), float64(maxDelay)))
	}

	//waitUntilUnregistered := func(r *Registry, jobID jobspb.JobID) {
	//	testutils.SucceedsSoon(t, func() error {
	//		r.mu.Lock()
	//		defer r.mu.Unlock()
	//		if _, ok := r.mu.adoptedJobs[jobID]; !ok {
	//			return nil
	//		}
	//		return errors.Errorf("waiting for the current resumer to unregister")
	//	})
	//}

	// Number of retries should be reasonably large such that they are sufficient
	// to test long delays but not too many to increase the test time.
	const retries = 50
	// initDelay and maxDelay can be large as we jump in time through a fake clock.
	initDelay := time.Second
	maxDelay := time.Hour

	const (
		// Job states to test, which explore different code paths in a job's lifecycle.
		running             = "running"
		revertingOnFailed   = "failed"
		revertingOnCanceled = "canceled"
		pauseRunning        = "pause-running"
		pauseReverting      = "pause-reverting"
		// To pause or cancel a job.
		pause  = true
		cancel = false
	)

	for _, testState := range []string{
		running, revertingOnFailed, revertingOnCanceled, pauseRunning, pauseReverting,
	} {
		t.Run(testState, func(t *testing.T) {
			ctx := context.Background()
			var done atomic.Value
			done.Store(false)
			// Buffered channel so that registry can execute without synchronization.
			transitionCh := make(chan struct{}, 3)

			// We use a manual clock to control and evaluate job execution times.
			// We initialize the clock with Now() because the job-creation timestamp,
			// 'created' column in system.jobs, of a new job is set from txn's time.
			clock := timeutil.NewManualTime(timeutil.Now())
			timeSource := hlc.NewClock(func() int64 {
				return clock.Now().UnixNano()
			}, base.DefaultMaxClockOffset)
			// Set up the test cluster.
			cs := clusterSettings(ctx, initDelay, maxDelay)
			args := base.TestServerArgs{
				Settings: cs,
				Knobs: base.TestingKnobs{
					JobsTestingKnobs: &TestingKnobs{
						TimeSource: timeSource,
						AfterJobStateMachine: func() {
							if done.Load().(bool) {
								return
							}
							transitionCh <- struct{}{}
						},
					},
				},
			}
			s, sqlDB, kvDB := serverutils.StartServer(t, args)
			defer s.Stopper().Stop(ctx)
			tdb := sqlutils.MakeSQLRunner(sqlDB)
			r := s.JobRegistry().(*Registry)

			RegisterConstructor(jobspb.TypeImport, func(job *Job, cs *cluster.Settings) Resumer {
				return FakeResumer{
					OnResume: func(ctx context.Context) error {
						if done.Load().(bool) {
							return nil
						}
						// The code path that we are testing.
						switch testState {
						case running:
							// Do not let the job succeed so that the job can be retried using
							// the exponential-backoff schedule.
							return NewRetryJobError("injecting error to retry running")
						case revertingOnFailed, pauseReverting:
							// Fail the job to cause it to revert.
							return errors.Errorf("injecting error to revert")
						case revertingOnCanceled:
							// We have to cancel the job to test reverting due to job cancelation.
							pauseOrCancelJob(t, ctx, kvDB, r, job.ID(), cancel)
							return nil
						case pauseRunning:
							//pauseOrCancelJob(t, ctx, kvDB, r, job.ID(), true)
							// We can return nil from here because a job is not marked as
							// succeeded if it's status is not running. After pausing, the
							// job's status is "pause-requested" in the jobs table.
							pauseOrCancelJob(t, ctx, kvDB, r, job.ID(), pause)
							return nil
						}
						assert.FailNow(t, "unknown status to test: %s", testState)
						return nil
					},
					FailOrCancel: func(ctx context.Context) error {
						if done.Load().(bool) {
							return nil
						}
						if testState == pauseReverting {
							pauseOrCancelJob(t, ctx, kvDB, r, job.ID(), pause)
							// If we return nil here, the job will be marked as failed regardless of
							// the fact that it is currently pause-requested in the jobs table. This
							// is because we currently do not check the current status of a job before
							// marking it as failed.
						}
						return NewRetryJobError("injecting error in reverting state")
					},
				}
			})

			// This test works as follows:
			// We retry for a fixed number. For each retry:
			// - We first advance the clock such that it is a little behind the
			//   next retry time.
			// - We then wait until a few adopt-loops complete to ensure that
			//   the registry gets a chance to pick up a job if it can.
			// - We validate that the number of resumed jobs has not increased to
			//   ensure that a job is not started/resumed/reverted before its next
			//   retry time.
			// - We then advance the clock to the exact next retry time. Now the job
			//   can be retried in the next adopt-loop.
			// - We wait until the resumer completes one execution and the job needs
			//   to be picked up again in the next adopt-loop.
			// - Now we validate that resumedJobs counter has increment, which ensures
			//   that the job has completed only one cycle in this time.
			//
			// If retries do not happen based on exponential-backoff times, our counters
			// will not match, causing the test to fail in validateCount.

			adoptItrs := r.metrics.AdoptIterations
			// Counting the number of times jobs are resumed.
			resumed := r.metrics.ResumedJobs
			// Create a new job, which will not be claimed immediately as our clock's
			// time has not advanced yet.
			jobID := createJob(ctx, s, r, kvDB)
			// Expected number of jobs resumed.
			expResumed := int64(0)
			// Number of times the job is retried, expected to follow num_runs in jobs.system.
			retryCnt := 0
			// We need to adjust the clock to correctly calculate expected retry time of the job.
			var lastRun time.Time
			tdb.QueryRow(t,
				"SELECT created FROM system.jobs where id = $1", jobID,
			).Scan(&lastRun)
			// When testing revert state, we first have to let the job run and fail
			// once to reach the reverting state.
			if testState == revertingOnFailed || testState == revertingOnCanceled {
				// When we test the reverting state, we first need to run the job and
				// cancel it. So we advance the clock such that the job can be started.
				clock.AdvanceTo(lastRun)
				// Wait until the resumer completes one execution in the reverting state.
				<-transitionCh
				if testState == revertingOnCanceled {
					<-transitionCh
				}
				expResumed = resumed.Count()
				retryCnt = 1
			}

			for i := 0; i < retries; i++ {
				// Exponential delay in the next retry.
				delay := nextDelay(retryCnt, initDelay, maxDelay)
				// The delay must not exceed the max delay setting. It ensures that
				// we are expecting correct timings from our test code, which in turn
				// ensures that the jobs are resumed with correct exponential delays.
				require.GreaterOrEqual(t, maxDelay, delay, "delay exceeds the max")
				// Advance the clock such that it is before the next expected retry time.
				clock.AdvanceTo(lastRun.Add(delay - unitTime))
				// This allows adopt-loops to run for a few times, which ensures that
				// adopt-loops do not resume jobs without correctly following the job
				// schedules.
				waitUntilCount(t, adoptItrs, adoptItrs.Count()+2)
				// Validate that the job is not resumed yet.
				validateCounts(t, expResumed, resumed.Count())
				// Advance the clock by delta from the expected time of next retry.
				clock.Advance(unitTime)
				// Wait until the resumer completes its execution.
				<-transitionCh
				if testState == pauseRunning || testState == pauseReverting {
					waitUntilStatus(t, tdb, jobID, StatusPaused)
					require.NoError(t, r.Unpause(ctx, nil, jobID))
				}
				expResumed++
				retryCnt++
				// Validate that the job is resumed only once.
				validateCounts(t, expResumed, resumed.Count())
				lastRun = clock.Now()
			}
			done.Store(true)
			// Let the job be retried one more time.
			clock.Advance(nextDelay(retryCnt, initDelay, maxDelay))
			// Wait until the job completes.
			testutils.SucceedsSoon(t, func() error {
				var found Status
				tdb.QueryRow(t, "SELECT status FROM system.jobs WHERE id = $1", jobID).Scan(&found)
				if found.Terminal() {
					return nil
				}
				retryCnt++
				clock.Advance(nextDelay(retryCnt, initDelay, maxDelay))
				return errors.Errorf("waiting job %d to reach a terminal state, currently %s", jobID, found)
			})
		})
	}
}

// TestExponentialBackoffSettings tests the cluster settings of exponential backoff delays.
func TestExponentialBackoffSettings(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	for _, test := range [...]struct {
		name string // Test case ID.
		// The setting to test.
		settingKey string
		// The value of the setting to set.
		value time.Duration
	}{
		{
			name:       "backoff initial delay setting",
			settingKey: retryInitialDelaySettingKey,
			value:      2 * time.Millisecond,
		},
		{
			name:       "backoff max delay setting",
			settingKey: retryMaxDelaySettingKey,
			value:      2 * time.Millisecond,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			var tdb *sqlutils.SQLRunner
			var finished atomic.Value
			finished.Store(false)
			var intercepted atomic.Value
			intercepted.Store(false)
			intercept := func(orig, updated JobMetadata) error {
				// If this updated is not to mark as succeeded or the test has already failed.
				if updated.Status != StatusSucceeded {
					return nil
				}

				// If marking the first time, prevent the marking and update the cluster
				// setting based on test params. The setting value should be reduced
				// from a large value to a small value.
				if !intercepted.Load().(bool) {
					tdb.Exec(t, fmt.Sprintf("SET CLUSTER SETTING %s = '%v'", test.settingKey, test.value))
					intercepted.Store(true)
					return errors.Errorf("preventing the job from succeeding")
				}
				// Let the job to succeed. As we began with a long interval and prevented
				// the job than succeeding in the first attempt, its re-execution
				// indicates that the setting is updated successfully and is in effect.
				finished.Store(true)
				return nil
			}

			// Setup the test cluster.
			cs := cluster.MakeTestingClusterSettings()
			// Set a small adopt interval to reduce test time.
			adoptIntervalSetting.Override(ctx, &cs.SV, 2*time.Millisecond)
			// Begin with a long delay.
			retryInitialDelaySetting.Override(ctx, &cs.SV, time.Hour)
			retryMaxDelaySetting.Override(ctx, &cs.SV, time.Hour)
			args := base.TestServerArgs{
				Settings: cs,
				Knobs:    base.TestingKnobs{JobsTestingKnobs: &TestingKnobs{BeforeUpdate: intercept}},
			}
			s, sdb, kvDB := serverutils.StartServer(t, args)
			defer s.Stopper().Stop(ctx)
			tdb = sqlutils.MakeSQLRunner(sdb)
			// Create and run a dummy job.
			RegisterConstructor(jobspb.TypeImport, func(_ *Job, cs *cluster.Settings) Resumer {
				return FakeResumer{}
			})
			registry := s.JobRegistry().(*Registry)
			id := registry.MakeJobID()
			require.NoError(t, kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				_, err := registry.CreateJobWithTxn(ctx, Record{
					// Job does not accept an empty Details field, so arbitrarily provide
					// ImportDetails.
					Details:  jobspb.ImportDetails{},
					Progress: jobspb.ImportProgress{},
				}, id, txn)
				return err
			}))

			// Wait for the job to be succeed.
			testutils.SucceedsSoon(t, func() error {
				if finished.Load().(bool) {
					return nil
				}
				return errors.Errorf("waiting for the job to complete")
			})
		})
	}
}
