/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.commandline.CommandList;
import org.apache.ignite.internal.commandline.metric.MetricCommandArg;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetricImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.thread.IgniteThreadFactory;
import org.junit.Test;

import static java.lang.Long.parseLong;
import static java.lang.System.currentTimeMillis;
import static java.util.regex.Pattern.quote;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_THREAD_KEEP_ALIVE_TIME;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandList.METRIC;
import static org.apache.ignite.internal.commandline.TablePrinter.COLUMN_SEPARATOR;
import static org.apache.ignite.internal.commandline.systemview.SystemViewCommandArg.NODE_ID;
import static org.apache.ignite.internal.managers.communication.GridIoManager.COMM_METRICS;
import static org.apache.ignite.internal.processors.cache.ClusterCachesInfo.CACHES_VIEW;
import static org.apache.ignite.internal.processors.cache.GridCacheProcessor.CACHE_GRP_PAGE_LIST_VIEW;
import static org.apache.ignite.internal.processors.cache.persistence.DataStorageMetricsImpl.DATASTORAGE_METRIC_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.SNAPSHOT_METRICS;
import static org.apache.ignite.internal.processors.job.GridJobProcessor.JOBS_METRICS;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.CLUSTER_METRICS;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.IGNITE_METRICS;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.PME_METRICS;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.SYS_METRICS;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.THREAD_POOLS;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.TX_METRICS;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.SEPARATOR;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.cacheMetricsRegistryName;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.processors.service.IgniteServiceProcessor.SVCS_VIEW;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/** Tests output of {@link CommandList#METRIC} command. */
public class MetricCommandTest extends GridCommandHandlerClusterByClassAbstractTest {
    /** Command line argument for printing metric values. */
    private static final String CMD_METRIC = METRIC.text();

    /** Test node with 0 index. */
    private IgniteEx ignite0;

    /** Test node with 1 index. */
    private IgniteEx ignite1;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        injectTestSystemOut();

        autoConfirmation = false;

        ignite0 = ignite(0);
        ignite1 = ignite(1);
    }

    /** Tests command error output in case of mandatory metric name is omitted. */
    @Test
    public void testMetricNameMissedFailure() {
        assertContains(log, executeCommand(EXIT_CODE_INVALID_ARGUMENTS, CMD_METRIC),
            "The name of a metric(metric registry) is expected.");
    }

    /** Tests command error output in case value of {@link MetricCommandArg#NODE_ID} argument is omitted. */
    @Test
    public void testNodeIdMissedFailure() {
        assertContains(log, executeCommand(EXIT_CODE_INVALID_ARGUMENTS, CMD_METRIC, SVCS_VIEW, NODE_ID.argName()),
            "ID of the node from which metric values should be obtained is expected.");
    }

    /** Tests command error output in case value of {@link MetricCommandArg#NODE_ID} argument is invalid.*/
    @Test
    public void testInvalidNodeIdFailure() {
        assertContains(log,
            executeCommand(EXIT_CODE_INVALID_ARGUMENTS, CMD_METRIC, SYS_METRICS, NODE_ID.argName(), "invalid_node_id"),
            "Failed to parse " + NODE_ID.argName() +
                " command argument. String representation of \"java.util.UUID\" is exepected." +
                " For example: 123e4567-e89b-42d3-a456-556642440000"
        );
    }

    /** Tests command error output in case multiple metric names are specified. */
    @Test
    public void testMultipleSystemViewNamesFailure() {
        assertContains(log,
            executeCommand(EXIT_CODE_INVALID_ARGUMENTS, CMD_METRIC, SVCS_VIEW, CACHE_GRP_PAGE_LIST_VIEW),
            "Multiple metric(metric registry) names are not supported.");
    }

    /** Tests command error output in case {@link MetricCommandArg#NODE_ID} argument value refers to nonexistent node. */
    @Test
    public void testNonExistentNodeIdFailure() {
        String incorrectNodeId = UUID.randomUUID().toString();

        assertContains(log,
            executeCommand(EXIT_CODE_INVALID_ARGUMENTS, CMD_METRIC, "--node-id", incorrectNodeId, CACHES_VIEW),
            "Failed to perform operation.\nNode with id=" + incorrectNodeId + " not found");
    }

    /** Tests command output in case nonexistent metric name is specified. */
    @Test
    public void testNonExistentMetric() {
        assertContains(log, executeCommand(EXIT_CODE_OK, CMD_METRIC, IGNITE_METRICS + SEPARATOR),
            "No metric with specified name was found [name=" + IGNITE_METRICS + SEPARATOR + ']');

        assertContains(log, executeCommand(EXIT_CODE_OK, CMD_METRIC, "nonexistent.metric"),
            "No metric with specified name was found [name=nonexistent.metric]");
    }

    /** */
    @Test
    public void testCommunicationMetrics() {
        Map<String, String> metrics = metrics(ignite0, COMM_METRICS);

        assertTrue(Integer.parseInt(metrics.get(metricName(COMM_METRICS, "SentBytesCount"))) > 0);
        assertTrue(Integer.parseInt(metrics.get(metricName(COMM_METRICS, "ReceivedBytesCount"))) > 0);
        assertEquals("0", metrics.get(metricName(COMM_METRICS, "OutboundMessagesQueueSize")));
        assertTrue(Integer.parseInt(metrics.get(metricName(COMM_METRICS, "ReceivedMessagesCount"))) > 0);
        assertTrue(Integer.parseInt(metrics.get(metricName(COMM_METRICS, "SentMessagesCount"))) > 0);
    }

    /** */
    @Test
    public void testJobsMetrics() {
        Arrays.asList(
            "WaitingTime",
            "Canceled",
            "Active",
            "Started",
            "Rejected",
            "ExecutionTime",
            "ExecutionTime",
            "Waiting"
        ).forEach(name -> assertEquals("0", metrics(ignite0, JOBS_METRICS).get(metricName(JOBS_METRICS, name))));
    }

    /** */
    @Test
    public void testPmeMetrics() {
        Map<String, String> metrics = metrics(ignite0, PME_METRICS);

        assertFalse(metrics.get(metricName(PME_METRICS, "CacheOperationsBlockedDurationHistogram")).isEmpty());
        assertFalse(metrics.get(metricName(PME_METRICS, "DurationHistogram")).isEmpty());
        assertEquals("0", metrics.get(metricName(PME_METRICS, "CacheOperationsBlockedDuration")));
        assertEquals("0", metrics.get(metricName(PME_METRICS, "Duration")));
    }

    /** */
    @Test
    public void testClusterMetrics() {
        Map<String, String> metrics = metrics(ignite0, CLUSTER_METRICS);

        assertEquals("true", metrics.get(metricName(CLUSTER_METRICS, "Rebalanced")));
        assertEquals("2", metrics.get(metricName(CLUSTER_METRICS, "TotalServerNodes")));
        assertEquals("2", metrics.get(metricName(CLUSTER_METRICS, "ActiveBaselineNodes")));
        assertEquals("2", metrics.get(metricName(CLUSTER_METRICS, "TotalBaselineNodes")));
        assertEquals("1", metrics.get(metricName(CLUSTER_METRICS, "TotalClientNodes")));
    }

    /** */
    @Test
    public void testDataRegionMetrics() {
        String mRegName = metricName("io", "dataregion", "default");

        Map<String, String> metrics = metrics(ignite0, mRegName);

        Arrays.asList(
            "TotalAllocatedSize",
            "LargeEntriesPagesCount",
            "PagesReplaced",
            "PhysicalMemorySize",
            "CheckpointBufferSize",
            "PagesReplaceRate",
            "AllocationRate",
            "PagesRead",
            "OffHeapSize",
            "UsedCheckpointBufferSize",
            "OffheapUsedSize",
            "EmptyDataPages",
            "PagesFillFactor",
            "DirtyPages",
            "EvictionRate",
            "PagesWritten",
            "TotalAllocatedPages",
            "PagesReplaceAge",
            "PhysicalMemoryPages",
            "TotalThrottlingTime",
            "InitialSize",
            "MaxSize"
        ).forEach(name -> assertFalse(metrics.get(metricName(mRegName, name)).isEmpty()));

        DataRegionConfiguration cfg =
            ignite0.configuration().getDataStorageConfiguration().getDefaultDataRegionConfiguration();

        assertEquals(Long.toString(cfg.getInitialSize()), metric(ignite0, metricName(mRegName, "InitialSize")));
        assertEquals(Long.toString(cfg.getMaxSize()), metric(ignite0, metricName(mRegName, "MaxSize")));
    }

    /** */
    @Test
    public void testCacheMetrics() {
        ignite0.createCache("default");

        String mRegName = cacheMetricsRegistryName("default", false);

        Map<String, String> metrics = metrics(ignite0, mRegName);

        Arrays.asList(
            "CacheTxRollbacks",
            "CacheMisses",
            "PutTimeTotal",
            "OffHeapGets",
            "CacheRemovals",
            "EntryProcessorHits",
            "HeapEntriesCount",
            "RemoveTimeTotal",
            "CachePuts",
            "EntryProcessorMisses",
            "OffHeapBackupEntriesCount",
            "CacheHits",
            "EntryProcessorPuts",
            "TotalRebalancedBytes",
            "QuerySumTime",
            "EntryProcessorInvokeTimeNanos",
            "EstimatedRebalancingKeys",
            "RollbackTimeTotal",
            "OffHeapMisses",
            "GetTimeTotal",
            "QueryFailed",
            "EntryProcessorReadOnlyInvocations",
            "CommitTimeTotal",
            "RebalancingBytesRate",
            "QueryCompleted",
            "EvictingPartitionsLeft",
            "CacheGets",
            "OffHeapEvictions",
            "IndexRebuildKeyProcessed",
            "RebalancingKeysRate",
            "OffHeapEntriesCount",
            "OffHeapPuts",
            "CacheTxCommits",
            "CacheSize",
            "RebalancedKeys",
            "EntryProcessorRemovals",
            "OffHeapPrimaryEntriesCount",
            "CacheEvictions",
            "OffHeapRemovals",
            "QueryExecuted",
            "RebalanceClearingPartitionsLeft",
            "OffHeapHits",
            "EntryProcessorMaxInvocationTime",
            "EntryProcessorMinInvocationTime"
        ).forEach(name -> assertEquals("0", metrics.get(metricName(mRegName, name))));

        assertEquals("-1", metrics.get(metricName(mRegName, "RebalanceStartTime")));
        assertEquals("false", metrics.get(metricName(mRegName, "IsIndexRebuildInProgress")));
        assertTrue(Long.parseLong(metrics.get(metricName(mRegName, "QueryMinimalTime"))) > 0);

        Arrays.asList(
            "RemoveTime",
            "GetTime",
            "PutTime",
            "CommitTime",
            "RollbackTime"
        ).forEach(name -> assertEquals("[0, 0, 0, 0, 0, 0]", metrics.get(metricName(mRegName, name))));

        assertTrue(metrics.get(metricName(mRegName, "TxKeyCollisions")).isEmpty());
    }

    /** */
    @Test
    public void testHistogramMetrics() {
        String mRegName = "histogramTest";

        MetricRegistry mreg = ignite1.context().metric().registry(mRegName);

        long[] bounds = new long[] {50, 500};

        HistogramMetricImpl histogram = mreg.histogram("histogram", bounds, null);

        histogram.value(10);
        histogram.value(51);
        histogram.value(60);
        histogram.value(600);
        histogram.value(600);
        histogram.value(600);

        histogram = mreg.histogram("histogram_with_underscore", bounds, null);

        histogram.value(10);
        histogram.value(51);
        histogram.value(60);
        histogram.value(600);
        histogram.value(600);
        histogram.value(600);

        assertEquals("1", metric(ignite1, metricName(mRegName, "histogram_0_50")));
        assertEquals("2", metric(ignite1, metricName(mRegName, "histogram_50_500")));
        assertEquals("3", metric(ignite1, metricName(mRegName, "histogram_500_inf")));
        assertEquals("[1, 2, 3]", metric(ignite1, metricName(mRegName, "histogram")));

        assertEquals("1", metric(ignite1, metricName(mRegName, "histogram_with_underscore_0_50")));
        assertEquals("2", metric(ignite1, metricName(mRegName, "histogram_with_underscore_50_500")));
        assertEquals("3", metric(ignite1, metricName(mRegName, "histogram_with_underscore_500_inf")));
        assertEquals("[1, 2, 3]", metric(ignite1, metricName(mRegName, "histogram_with_underscore")));
    }

    /** */
    @Test
    public void testSystemMetrics() {
        Arrays.asList(
            "CurrentThreadCpuTime",
            "memory.heap.committed",
            "ThreadCount",
            "memory.nonheap.committed",
            "TotalStartedThreadCount",
            "CurrentThreadUserTime",
            "PeakThreadCount",
            "memory.nonheap.used",
            "memory.heap.used",
            "memory.nonheap.max",
            "TotalExecutedTasks",
            "SystemLoadAverage",
            "memory.heap.init",
            "UpTime",
            "DaemonThreadCount",
            "CpuLoad",
            "GcCpuLoad",
            "memory.heap.max",
            "memory.nonheap.init"
        ).forEach(name -> assertFalse(metrics(ignite0, SYS_METRICS).get(metricName(SYS_METRICS, name)).isEmpty()));
    }

    /** */
    @Test
    public void testThreadPoolMetrics() {
        String mRegName = metricName(THREAD_POOLS, "GridSystemExecutor");

        Map<String, String> metrics = metrics(ignite0, mRegName);

        Arrays.asList(
            "MaximumPoolSize",
            "Terminated",
            "QueueSize",
            "KeepAliveTime",
            "RejectedExecutionHandlerClass",
            "ThreadFactoryClass",
            "CompletedTaskCount",
            "Terminating",
            "Shutdown",
            "ActiveCount",
            "LargestPoolSize",
            "PoolSize",
            "CorePoolSize",
            "TaskCount"
        ).forEach(name -> assertFalse(metrics.get(metricName(mRegName, name)).isEmpty()));

        assertEquals(AbortPolicy.class.getName(), metrics.get(metricName(mRegName, "RejectedExecutionHandlerClass")));
        assertEquals(IgniteThreadFactory.class.getName(), metrics.get(metricName(mRegName, "ThreadFactoryClass")));
        assertEquals("false", metrics.get(metricName(mRegName, "Terminating")));
        assertEquals("false", metrics.get(metricName(mRegName, "Shutdown")));
        assertEquals("false", metrics.get(metricName(mRegName, "Terminated")));
        assertEquals(Long.toString(DFLT_THREAD_KEEP_ALIVE_TIME), metrics.get(metricName(mRegName, "KeepAliveTime")));
    }

    /** */
    @Test
    public void testDataStorageMetrics() {
        Map<String, String> metrics = metrics(ignite0, DATASTORAGE_METRIC_PREFIX);

        Arrays.asList(
            "LastCheckpointTotalPagesNumber",
            "WalFsyncTimeDuration",
            "WalBuffPollSpinsRate",
            "WalTotalSize",
            "StorageSize",
            "LastCheckpointCopiedOnWritePagesNumber",
            "LastCheckpointMarkDuration",
            "SparseStorageSize",
            "LastCheckpointPagesWriteDuration",
            "CheckpointTotalTime",
            "WalLastRollOverTime",
            "LastCheckpointLockWaitDuration",
            "WalArchiveSegments",
            "WalFsyncTimeNum",
            "LastCheckpointDataPagesNumber",
            "LastCheckpointFsyncDuration",
            "WalWritingRate",
            "LastCheckpointDuration"
        ).forEach(name -> assertEquals("0", metrics.get(metricName(DATASTORAGE_METRIC_PREFIX, name))));

        assertTrue(Integer.parseInt(metrics.get(metricName(DATASTORAGE_METRIC_PREFIX, "WalLoggingRate"))) > 0);
    }

    /** */
    @Test
    public void testIgniteKernalMetrics() {
        Map<String, String> metrics = metrics(ignite0, IGNITE_METRICS);

        Arrays.asList(
            "fullVersion",
            "copyright",
            "osInformation",
            "jdkInformation",
            "vmName",
            "discoverySpiFormatted",
            "communicationSpiFormatted",
            "deploymentSpiFormatted",
            "checkpointSpiFormatted",
            "collisionSpiFormatted",
            "eventStorageSpiFormatted",
            "failoverSpiFormatted",
            "loadBalancingSpiFormatted",
            "startTimestampFormatted",
            "uptimeFormatted"
        ).forEach(name -> assertFalse(metrics.get(metricName(IGNITE_METRICS, name)).isEmpty()));

        assertEquals(System.getProperty("user.name"), metrics.get(metricName(IGNITE_METRICS, "osUser")));

        assertEquals("true", metrics.get(metricName(IGNITE_METRICS, "isRebalanceEnabled")));
        assertEquals("true", metrics.get(metricName(IGNITE_METRICS, "isNodeInBaseline")));
        assertEquals("true", metrics.get(metricName(IGNITE_METRICS, "active")));

        assertTrue(parseLong(metrics.get(metricName(IGNITE_METRICS, "startTimestamp"))) > 0);
        assertTrue(parseLong(metrics.get(metricName(IGNITE_METRICS, "uptime"))) > 0);

        assertEquals(ignite0.name(), metrics.get(metricName(IGNITE_METRICS, "instanceName")));

        assertEquals("[]", metrics.get(metricName(IGNITE_METRICS, "userAttributesFormatted")));
        assertEquals("[]", metrics.get(metricName(IGNITE_METRICS, "lifecycleBeansFormatted")));
        assertEquals("{}", metrics.get(metricName(IGNITE_METRICS, "longJVMPauseLastEvents")));

        assertEquals("0", metrics.get(metricName(IGNITE_METRICS, "longJVMPausesCount")));
        assertEquals("0", metrics.get(metricName(IGNITE_METRICS, "longJVMPausesTotalDuration")));

        long clusterStateChangeTime = parseLong(metrics.get(
            metricName(IGNITE_METRICS, "lastClusterStateChangeTime")));

        assertTrue(0 < clusterStateChangeTime && clusterStateChangeTime < currentTimeMillis());

        assertEquals(String.valueOf(ignite0.configuration().getPublicThreadPoolSize()),
            metrics.get(metricName(IGNITE_METRICS, "executorServiceFormatted")));

        assertEquals(Boolean.toString(ignite0.configuration().isPeerClassLoadingEnabled()),
            metrics.get(metricName(IGNITE_METRICS, "isPeerClassLoadingEnabled")));

        assertTrue(metrics.get(metricName(IGNITE_METRICS, "currentCoordinatorFormatted"))
            .contains(ignite0.localNode().id().toString()));

        assertEquals(ignite0.configuration().getIgniteHome(), metrics.get(metricName(IGNITE_METRICS, "igniteHome")));

        assertEquals(ignite0.localNode().id().toString(), metrics.get(metricName(IGNITE_METRICS, "localNodeId")));

        assertEquals(ignite0.configuration().getGridLogger().toString(),
            metrics.get(metricName(IGNITE_METRICS, "gridLoggerFormatted")));

        assertEquals(ignite0.configuration().getMBeanServer().toString(),
            metrics.get(metricName(IGNITE_METRICS, "mBeanServerFormatted")));

        assertEquals(ACTIVE.toString(), metrics.get(metricName(IGNITE_METRICS, "clusterState")));
    }

    /** */
    @Test
    public void testSnapshotMetrics() {
        assertThrowsWithCause(
            () -> ignite0.snapshot().createSnapshot("test_snapshot").get(), IgniteCheckedException.class);

        Map<String, String> metrics = metrics(ignite0, SNAPSHOT_METRICS);

        assertEquals("test_snapshot", metrics.get(metricName(SNAPSHOT_METRICS, "LastSnapshotName")));
        assertEquals("Snapshot operation has not been fully completed [err={}, snpReq=null]",
            metrics.get(metricName(SNAPSHOT_METRICS, "LastSnapshotErrorMessage")));
        assertTrue(parseLong(metrics.get(metricName(SNAPSHOT_METRICS, "LastSnapshotStartTime"))) < currentTimeMillis());
        assertTrue(parseLong(metrics.get(metricName(SNAPSHOT_METRICS, "LastSnapshotEndTime"))) < currentTimeMillis());
        assertEquals("[]", metrics.get(metricName(SNAPSHOT_METRICS, "LocalSnapshotNames")));
    }

    /** */
    @Test
    public void testTxMetrics() {
        Map<String, String> metrics = metrics(ignite0, TX_METRICS);

        Arrays.asList(
            "txRollbacks",
            "commitTime",
            "totalNodeSystemTime",
            "rollbackTime",
            "OwnerTransactionsNumber",
            "totalNodeUserTime",
            "txCommits",
            "TransactionsHoldingLockNumber",
            "LockedKeysNumber"
        ).forEach(name -> assertEquals("0", metrics.get(metricName(TX_METRICS, name))));

        assertEquals("{}", metrics.get(metricName(TX_METRICS, "AllOwnerTransactions")));

        assertEquals("[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]",
            metrics.get(metricName(TX_METRICS, "nodeSystemTimeHistogram")));

        assertEquals("[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]",
            metrics.get(metricName(TX_METRICS, "nodeUserTimeHistogram")));
    }

    /**
     * Gets metric values via command-line utility.
     *
     * @param node Node to obtain metric values from.
     * @param name Name of a particular metric or metric registry.
     * @return String representation of metric values.
     */
    private Map<String, String> metrics(IgniteEx node, String name) {
        String nodeId = node.context().discovery().localNode().id().toString();

        String out = executeCommand(EXIT_CODE_OK, CMD_METRIC, name, NODE_ID.argName(), nodeId);

        Map<String, String> res = parseMetricCommandOutput(out);

        assertEquals("value", res.remove("metric"));
        
        return res;
    }

    /**
     * Gets single metric value via command-line utility.
     *
     * @param node Node to obtain metric from.
     * @param name Name of the metric.
     * @return String representation of metric value.
     */
    private String metric(IgniteEx node, String name) {
        Map<String, String> metrics = metrics(node, name);
        
        assertEquals(1, metrics.size());
        
        return metrics.get(name);
    }

    /**
     * Obtains metric values from command output.
     *
     * @param out Command output to parse.
     * @return System view values.
     */
    private Map<String, String> parseMetricCommandOutput(String out) {
        String outStart = "--------------------------------------------------------------------------------";

        String outEnd = "Command [" + METRIC.toCommandName() + "] finished with code: " + EXIT_CODE_OK;

        String[] rows = out.substring(
            out.indexOf(outStart) + outStart.length() + 1,
            out.indexOf(outEnd) - 1
        ).split(U.nl());

        Map<String, String> res = new HashMap<>();

        for (String row : rows) {
            Iterator<String> iter = Arrays.stream(row.split(quote(COLUMN_SEPARATOR)))
                .map(String::trim)
                .filter(str -> !str.isEmpty())
                .iterator();

            res.put(iter.next(), iter.hasNext() ? iter.next() : "");
        }
        
        return res;
    }

    /**
     * Executes command and checks its exit code.
     *
     * @param expExitCode Expected exit code.
     * @param args Command lines arguments.
     * @return Result of command execution.
     */
    private String executeCommand(int expExitCode, String... args) {
        int res = execute(args);

        assertEquals(expExitCode, res);

        return testOut.toString();
    }
}
