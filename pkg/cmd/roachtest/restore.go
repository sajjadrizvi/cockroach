// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"net/http"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// HealthChecker runs a regular check that verifies that a specified subset
// of (CockroachDB) nodes look "very healthy". That is, there are no stuck
// proposals, liveness problems, or whatever else might get added in the
// future.
type HealthChecker struct {
	c      *cluster
	nodes  nodeListOption
	doneCh chan struct{}
}

// NewHealthChecker returns a populated HealthChecker.
func NewHealthChecker(c *cluster, nodes nodeListOption) *HealthChecker {
	return &HealthChecker{
		c:      c,
		nodes:  nodes,
		doneCh: make(chan struct{}),
	}
}

// Done signals the HeatlthChecker's Runner to shut down.
func (hc *HealthChecker) Done() {
	close(hc.doneCh)
}

type gossipAlert struct {
	NodeID, StoreID       int
	Category, Description string
	Value                 float64
}

type gossipAlerts []gossipAlert

func (g gossipAlerts) String() string {
	var buf bytes.Buffer
	tw := tabwriter.NewWriter(&buf, 2, 1, 2, ' ', 0)

	for _, a := range g {
		fmt.Fprintf(tw, "n%d/s%d\t%.2f\t%s\t%s\n", a.NodeID, a.StoreID, a.Value, a.Category, a.Description)
	}
	_ = tw.Flush()
	return buf.String()
}

// Runner makes sure the gossip_alerts table is empty at all times.
//
// TODO(tschottdorf): actually let this fail the test instead of logging complaints.
func (hc *HealthChecker) Runner(ctx context.Context) (err error) {
	logger, err := hc.c.l.ChildLogger("health")
	if err != nil {
		return err
	}
	defer func() {
		logger.Printf("health check terminated with %v\n", err)
	}()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-hc.doneCh:
			return nil
		case <-ticker.C:
		}

		tBegin := timeutil.Now()

		nodeIdx := 1 + rand.Intn(len(hc.nodes))
		db, err := hc.c.ConnE(ctx, nodeIdx)
		if err != nil {
			return err
		}
		// TODO(tschottdorf): remove replicate queue failures when the cluster first starts.
		// Ditto queue.raftsnapshot.process.failure.
		rows, err := db.QueryContext(ctx, `SELECT * FROM crdb_internal.gossip_alerts ORDER BY node_id ASC, store_id ASC`)
		_ = db.Close()
		if err != nil {
			return err
		}
		var rr gossipAlerts
		for rows.Next() {
			a := gossipAlert{StoreID: -1}
			var storeID gosql.NullInt64
			if err := rows.Scan(&a.NodeID, &storeID, &a.Category, &a.Description, &a.Value); err != nil {
				return err
			}
			if storeID.Valid {
				a.StoreID = int(storeID.Int64)
			}
			rr = append(rr, a)
		}
		if len(rr) > 0 {
			logger.Printf(rr.String() + "\n")
			// TODO(tschottdorf): see method comment.
			// return errors.New(rr.String())
		}

		if elapsed := timeutil.Since(tBegin); elapsed > 10*time.Second {
			err := errors.Errorf("health check against node %d took %s", nodeIdx, elapsed)
			logger.Printf("%+v", err)
			// TODO(tschottdorf): see method comment.
			// return err
		}
	}
}

// DiskUsageLogger regularly logs the disk spaced used by the nodes in the cluster.
type DiskUsageLogger struct {
	c      *cluster
	doneCh chan struct{}
}

// NewDiskUsageLogger populates a DiskUsageLogger.
func NewDiskUsageLogger(c *cluster) *DiskUsageLogger {
	return &DiskUsageLogger{
		c:      c,
		doneCh: make(chan struct{}),
	}
}

// Done instructs the Runner to terminate.
func (dul *DiskUsageLogger) Done() {
	close(dul.doneCh)
}

// Runner runs in a loop until Done() is called and prints the cluster-wide per
// node disk usage in descending order.
func (dul *DiskUsageLogger) Runner(ctx context.Context) error {
	logger, err := dul.c.l.ChildLogger("diskusage")
	if err != nil {
		return err
	}
	quietLogger, err := dul.c.l.ChildLogger("diskusage-exec", quietStdout, quietStderr)
	if err != nil {
		return err
	}

	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-dul.doneCh:
			return nil
		case <-ticker.C:
		}

		type usage struct {
			nodeNum int
			bytes   int
		}

		var bytesUsed []usage
		for i := 1; i <= dul.c.spec.NodeCount; i++ {
			cur, err := getDiskUsageInBytes(ctx, dul.c, quietLogger, i)
			if err != nil {
				// This can trigger spuriously as compactions remove files out from under `du`.
				logger.Printf("%s", errors.Wrapf(err, "node #%d", i))
				cur = -1
			}
			bytesUsed = append(bytesUsed, usage{
				nodeNum: i,
				bytes:   cur,
			})
		}
		sort.Slice(bytesUsed, func(i, j int) bool { return bytesUsed[i].bytes > bytesUsed[j].bytes }) // descending

		var s []string
		for _, usage := range bytesUsed {
			s = append(s, fmt.Sprintf("n#%d: %s", usage.nodeNum, humanizeutil.IBytes(int64(usage.bytes))))
		}

		logger.Printf("%s\n", strings.Join(s, ", "))
	}
}
func registerRestoreNodeShutdown(r *testRegistry) {
	makeRestoreStarter := func(ctx context.Context, t *test, c *cluster, gatewayNode int) jobStarter {
		return func(c *cluster) (string, error) {
			t.l.Printf("connecting to gateway")
			gatewayDB := c.Conn(ctx, gatewayNode)
			defer gatewayDB.Close()

			t.l.Printf("creating bank database")
			if _, err := gatewayDB.Exec("CREATE DATABASE bank"); err != nil {
				return "", err
			}

			errCh := make(chan error, 1)
			go func() {
				defer close(errCh)

				// 10 GiB restore.
				restoreQuery := `RESTORE bank.bank FROM
					'gs://cockroach-fixtures/workload/bank/version=1.0.0,payload-bytes=100,ranges=10,rows=10000000,seed=1/bank'`

				t.l.Printf("starting to run the restore job")
				if _, err := gatewayDB.Exec(restoreQuery); err != nil {
					errCh <- err
				}
				t.l.Printf("done running restore job")
			}()

			// Wait for the job.
			retryOpts := retry.Options{
				MaxRetries:     50,
				InitialBackoff: 1 * time.Second,
				MaxBackoff:     5 * time.Second,
			}
			for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
				var jobCount int
				if err := gatewayDB.QueryRowContext(ctx, "SELECT count(*) FROM [SHOW JOBS] WHERE job_type = 'RESTORE'").Scan(&jobCount); err != nil {
					return "", err
				}

				select {
				case err := <-errCh:
					// We got an error when starting the job.
					return "", err
				default:
				}

				if jobCount == 0 {
					t.l.Printf("waiting for restore job")
				} else if jobCount == 1 {
					t.l.Printf("found restore job")
					break
				} else {
					t.l.Printf("found multiple restore jobs -- erroring")
					return "", errors.New("unexpectedly found multiple restore jobs")
				}
			}

			var jobID string
			if err := gatewayDB.QueryRowContext(ctx, "SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'RESTORE'").Scan(&jobID); err != nil {
				return "", errors.Wrap(err, "querying the job ID")
			}
			return jobID, nil
		}
	}

	r.Add(testSpec{
		Name:       "restore/nodeShutdown/worker",
		Owner:      OwnerBulkIO,
		Cluster:    makeClusterSpec(4),
		MinVersion: "v21.1.0",
		Run: func(ctx context.Context, t *test, c *cluster) {
			gatewayNode := 2
			nodeToShutdown := 3
			c.Put(ctx, cockroach, "./cockroach")
			c.Start(ctx, t)

			jobSurvivesNodeShutdown(ctx, t, c, nodeToShutdown, makeRestoreStarter(ctx, t, c, gatewayNode))
		},
	})

	r.Add(testSpec{
		Name:       "restore/nodeShutdown/coordinator",
		Owner:      OwnerBulkIO,
		Cluster:    makeClusterSpec(4),
		MinVersion: "v21.1.0",
		Run: func(ctx context.Context, t *test, c *cluster) {
			gatewayNode := 2
			nodeToShutdown := 2
			c.Put(ctx, cockroach, "./cockroach")
			c.Start(ctx, t)

			jobSurvivesNodeShutdown(ctx, t, c, nodeToShutdown, makeRestoreStarter(ctx, t, c, gatewayNode))
		},
	})
}

func registerRestore(r *testRegistry) {
	largeVolumeSize := 2500 // the size in GB of disks in large volume configs

	for _, item := range []struct {
		nodes        int
		cpus         int
		largeVolumes bool

		timeout time.Duration
	}{
		{nodes: 10, timeout: 6 * time.Hour},
		{nodes: 32, timeout: 3 * time.Hour},
		{nodes: 6, timeout: 4 * time.Hour, cpus: 16, largeVolumes: true},
	} {
		clusterOpts := make([]createOption, 0)
		testName := fmt.Sprintf("restore2TB/nodes=%d", item.nodes)
		if item.cpus != 0 {
			clusterOpts = append(clusterOpts, cpu(item.cpus))
			testName += fmt.Sprintf("/cpus=%d", item.cpus)
		}
		if item.largeVolumes {
			clusterOpts = append(clusterOpts, volumeSize(largeVolumeSize))
			testName += fmt.Sprintf("/pd-volume=%dGB", largeVolumeSize)
		}

		r.Add(testSpec{
			Name:    testName,
			Owner:   OwnerBulkIO,
			Cluster: makeClusterSpec(item.nodes, clusterOpts...),
			Timeout: item.timeout,
			Run: func(ctx context.Context, t *test, c *cluster) {
				// Randomize starting with encryption-at-rest enabled.
				c.encryptAtRandom = true
				c.Put(ctx, cockroach, "./cockroach")
				c.Start(ctx, t)
				m := newMonitor(ctx, c)

				// Run the disk usage logger in the monitor to guarantee its
				// having terminated when the test ends.
				dul := NewDiskUsageLogger(c)
				m.Go(dul.Runner)
				hc := NewHealthChecker(c, c.All())
				m.Go(hc.Runner)

				// TODO(peter): This currently causes the test to fail because we see a
				// flurry of valid merges when the restore finishes.
				//
				// m.Go(func(ctx context.Context) error {
				// 	// Make sure the merge queue doesn't muck with our restore.
				// 	return verifyMetrics(ctx, c, map[string]float64{
				// 		"cr.store.queue.merge.process.success": 10,
				// 		"cr.store.queue.merge.process.failure": 10,
				// 	})
				// })

				tick := initBulkJobPerfArtifacts(ctx, testName, item.timeout)
				m.Go(func(ctx context.Context) error {
					defer dul.Done()
					defer hc.Done()
					t.Status(`running restore`)
					c.Run(ctx, c.Node(1), `./cockroach sql --insecure -e "CREATE DATABASE restore2tb"`)
					// Tick once before starting the restore, and once after to capture the
					// total elapsed time. This is used by roachperf to compute and display
					// the average MB/sec per node.
					tick()
					c.Run(ctx, c.Node(1), `./cockroach sql --insecure -e "
				RESTORE csv.bank FROM
				'gs://cockroach-fixtures/workload/bank/version=1.0.0,payload-bytes=10240,ranges=0,rows=65104166,seed=1/bank'
				WITH into_db = 'restore2tb'"`)
					tick()

					// Upload the perf artifacts to any one of the nodes so that the test
					// runner copies it into an appropriate directory path.
					if err := c.PutE(ctx, c.l, perfArtifactsDir, perfArtifactsDir, c.Node(1)); err != nil {
						log.Errorf(ctx, "failed to upload perf artifacts to node: %s", err.Error())
					}
					return nil
				})
				m.Wait()
			},
		})
	}
}

// verifyMetrics loops, retrieving the timeseries metrics specified in m every
// 10s and verifying that the most recent value is less that the limit
// specified in m. This is particularly useful for verifying that a counter
// metric does not exceed some threshold during a test. For example, the
// restore and import tests verify that the range merge queue is inactive.
func verifyMetrics(ctx context.Context, c *cluster, m map[string]float64) error {
	const sample = 10 * time.Second
	// Query needed information over the timespan of the query.
	url := "http://" + c.ExternalAdminUIAddr(ctx, c.Node(1))[0] + "/ts/query"

	request := tspb.TimeSeriesQueryRequest{
		// Ask for one minute intervals. We can't just ask for the whole hour
		// because the time series query system does not support downsampling
		// offsets.
		SampleNanos: sample.Nanoseconds(),
	}
	for name := range m {
		request.Queries = append(request.Queries, tspb.Query{
			Name:             name,
			Downsampler:      tspb.TimeSeriesQueryAggregator_AVG.Enum(),
			SourceAggregator: tspb.TimeSeriesQueryAggregator_SUM.Enum(),
		})
	}

	ticker := time.NewTicker(sample)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}

		now := timeutil.Now()
		request.StartNanos = now.Add(-sample * 3).UnixNano()
		request.EndNanos = now.UnixNano()

		var response tspb.TimeSeriesQueryResponse
		if err := httputil.PostJSON(http.Client{}, url, &request, &response); err != nil {
			return err
		}

		for i := range request.Queries {
			name := request.Queries[i].Name
			data := response.Results[i].Datapoints
			n := len(data)
			if n == 0 {
				continue
			}
			limit := m[name]
			value := data[n-1].Value
			if value >= limit {
				return fmt.Errorf("%s: %.1f >= %.1f @ %d", name, value, limit, data[n-1].TimestampNanos)
			}
		}
	}
}

// TODO(peter): silence unused warning.
var _ = verifyMetrics
