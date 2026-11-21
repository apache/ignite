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

package org.apache.ignite.internal.benchmarks.jmh.profile;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import com.sun.management.ThreadMXBean;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.util.nio.GridNioServer;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.profile.ProfilerException;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;

import static org.apache.ignite.internal.benchmarks.jmh.profile.JmhProfilerUtils.group;
import static org.apache.ignite.internal.benchmarks.jmh.profile.JmhProfilerUtils.label;
import static org.apache.ignite.internal.benchmarks.jmh.profile.JmhProfilerUtils.result;
import static org.apache.ignite.internal.benchmarks.jmh.profile.JmhProfilerUtils.writeRunDescription;
import static org.apache.ignite.internal.managers.discovery.GridDiscoveryManager.DISCO_METRICS;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.THREAD_POOLS;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.COMMUNICATION_METRICS_GROUP_NAME;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.RECEIVED_MESSAGES_METRIC_NAME;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.SENT_MESSAGES_BY_TYPE_METRIC_NAME;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.SENT_MESSAGES_METRIC_NAME;

/** Bytes allocated per operation by thread group, and messages and tasks per operation by node. */
public class JmhAllocationProfiler implements InternalProfiler {
    /** */
    private static final ThreadMXBean THREADS = (ThreadMXBean)ManagementFactory.getThreadMXBean();

    /** Processed discovery messages. */
    private static final String DISCO_PROCESSED_MESSAGES = "TotalProcessedMessages";

    /** Prefix of the metrics of messages sent by type; the name ends with the message's direct type. */
    private static final String SENT_BY_TYPE = metricName(COMMUNICATION_METRICS_GROUP_NAME,
        SENT_MESSAGES_BY_TYPE_METRIC_NAME) + '.';

    /** Snapshot at the start of the iteration. */
    private Snapshot start;

    /** Snapshot taken before the benchmark stopped its nodes, if it did in this iteration. */
    private Snapshot beforeStop;

    /** Labels of the counter results reported so far. */
    private final Set<String> reported = new HashSet<>();

    /** @param initLine Empty, or {@code dir=<path>} to write the run's JDK and OS to {@code <path>/run.properties}. */
    public JmhAllocationProfiler(String initLine) throws ProfilerException {
        if (initLine.startsWith("dir="))
            writeRunDescription(Paths.get(initLine.substring("dir=".length())));
        else if (!initLine.isEmpty())
            throw new ProfilerException("Unknown option, expected dir=<path>: " + initLine);

        JmhNodeStopHook.set(JmhAllocationProfiler.class, () -> beforeStop = Snapshot.take());
    }

    /** {@inheritDoc} */
    @Override public void beforeIteration(BenchmarkParams benchParams, IterationParams iterParams) {
        beforeStop = null;
        start = Snapshot.take();
    }

    /** {@inheritDoc} */
    @Override public Collection<? extends Result> afterIteration(
        BenchmarkParams benchParams,
        IterationParams iterParams,
        IterationResult res
    ) {
        Snapshot end = beforeStop != null ? beforeStop : Snapshot.take();

        if (!start.clients.isEmpty() && end.clients.isEmpty())
            throw new IllegalStateException(JmhNodeStopHook.NOT_CALLED);

        // The snapshots cover the whole iteration, so divide by all its operations, not only the measured ones.
        long ops = res.getMetadata().getAllOps();

        if (ops == 0)
            return List.of();

        List<Result> out = bytesPerGroup(end, ops);

        out.addAll(counters(end, ops));

        return out;
    }

    /** {@inheritDoc} */
    @Override public String getDescription() {
        return "Bytes allocated per operation by node and thread pool, and messages and tasks per operation by node.";
    }

    /** @return Bytes per operation of each thread group and of all of them. */
    private List<Result> bytesPerGroup(Snapshot end, long ops) {
        long self = Thread.currentThread().getId();

        Map<String, Long> grps = new TreeMap<>();

        for (Map.Entry<Long, Long> e : end.bytes.entrySet()) {
            if (e.getKey() == self)
                continue;

            long startBytes = start.bytes.getOrDefault(e.getKey(), 0L);

            grps.merge(group(end.names.get(e.getKey()), end.clients), e.getValue() - startBytes, Long::sum);
        }

        List<Result> out = new ArrayList<>();

        grps.forEach((grp, bytes) -> out.add(result("alloc.norm:" + grp, bytes, ops, "B/op")));

        out.add(result("alloc.norm:total", grps.values().stream().mapToLong(Long::longValue).sum(), ops, "B/op"));

        return out;
    }

    /** @return Node counters per operation; a counter missing at the start gives no result. */
    private List<Result> counters(Snapshot end, long ops) {
        List<Result> out = new ArrayList<>();

        for (Counter c : Counter.values()) {
            Map<String, Long> startVals = start.counters.get(c);

            for (Map.Entry<String, Long> e : end.counters.get(c).entrySet()) {
                String lbl = e.getKey();
                Long startVal = startVals.get(lbl);

                if (startVal == null)
                    continue;

                if (c.sparse && e.getValue().equals(startVal) && !reported.contains(lbl))
                    continue;

                reported.add(lbl);

                out.add(result(lbl, e.getValue() - startVal, ops, c.unit));
            }
        }

        return out;
    }

    /** Node counters the profiler reports per operation. */
    private enum Counter {
        /** Communication messages sent. */
        COMM_MSGS("comm.msgs.norm:", "msgs/op", false),

        /** Communication bytes sent. */
        COMM_BYTES("comm.bytes.norm:", "B/op", false),

        /** Communication messages received. */
        COMM_RCVD("comm.rcvd.norm:", "msgs/op", false),

        /** Communication messages sent, by message class. */
        COMM_TYPE("comm.type.norm:", "msgs/op", true),

        /** Discovery messages processed. */
        DISCO_MSGS("disco.msgs.norm:", "msgs/op", false),

        /** Tasks completed, by thread pool. */
        POOL_TASKS("pool.tasks.norm:", "tasks/op", true);

        /** Result label prefix. */
        final String prefix;

        /** Result unit. */
        final String unit;

        /** Reported from the first iteration it moved in: a node has hundreds of these, most of them idle. */
        final boolean sparse;

        /** */
        Counter(String prefix, String unit, boolean sparse) {
            this.prefix = prefix;
            this.unit = unit;
            this.sparse = sparse;
        }
    }

    /** Per-thread allocated bytes and per-node counters at one moment. */
    private static class Snapshot {
        /** Thread ID to allocated bytes. */
        final Map<Long, Long> bytes = new HashMap<>();

        /** Thread ID to name. */
        final Map<Long, String> names = new HashMap<>();

        /** Result label to value, by counter. */
        final Map<Counter, Map<String, Long>> counters = new EnumMap<>(Counter.class);

        /** Node name to client flag. */
        final Map<String, Boolean> clients = new HashMap<>();

        /** */
        private Snapshot() {
            for (Counter c : Counter.values())
                counters.put(c, new HashMap<>());
        }

        /** @return Snapshot of the current state. */
        static Snapshot take() {
            Snapshot s = new Snapshot();

            s.readThreads();

            for (Ignite ignite : Ignition.allGrids()) {
                if (ignite.name() != null)
                    s.readNode((IgniteEx)ignite);
            }

            return s;
        }

        /** Reads the allocated bytes and the name of each live thread. */
        private void readThreads() {
            long[] ids = THREADS.getAllThreadIds();
            long[] allocated = THREADS.getThreadAllocatedBytes(ids);
            ThreadInfo[] infos = THREADS.getThreadInfo(ids);

            for (int i = 0; i < ids.length; i++) {
                if (allocated[i] >= 0 && infos[i] != null) {
                    bytes.put(ids[i], allocated[i]);
                    names.put(ids[i], infos[i].getThreadName());
                }
            }
        }

        /** Reads the client flag and the counters of a node. */
        private void readNode(IgniteEx ignite) {
            String node = ignite.name();

            clients.put(node, ignite.cluster().localNode().isClient());

            GridMetricManager mmgr = ignite.context().metric();
            ReadOnlyMetricRegistry comm = mmgr.registry(COMMUNICATION_METRICS_GROUP_NAME);

            put(Counter.COMM_MSGS, node, comm.findMetric(SENT_MESSAGES_METRIC_NAME));
            put(Counter.COMM_BYTES, node, comm.findMetric(GridNioServer.SENT_BYTES_METRIC_NAME));
            put(Counter.COMM_RCVD, node, comm.findMetric(RECEIVED_MESSAGES_METRIC_NAME));
            put(Counter.DISCO_MSGS, node, mmgr.registry(DISCO_METRICS).findMetric(DISCO_PROCESSED_MESSAGES));

            readMessageTypes(ignite, comm);
            readPools(node, mmgr);
        }

        /** Reads the messages a node sent by message class. */
        private void readMessageTypes(IgniteEx ignite, ReadOnlyMetricRegistry comm) {
            for (Metric m : comm) {
                if (m.name().startsWith(SENT_BY_TYPE)) {
                    short type = Short.parseShort(m.name().substring(SENT_BY_TYPE.length()));
                    String cls = ignite.context().messageFactory().create(type).getClass().getSimpleName();

                    put(Counter.COMM_TYPE, ignite.name() + ':' + cls, m);
                }
            }
        }

        /** Reads the tasks each thread pool of a node completed. */
        private void readPools(String node, GridMetricManager mmgr) {
            for (ReadOnlyMetricRegistry mreg : mmgr) {
                if (mreg.name().startsWith(THREAD_POOLS + '.')) {
                    // IgniteStripedExecutor counts completed tasks under another name.
                    Metric m = mreg.findMetric("CompletedTaskCount");

                    put(Counter.POOL_TASKS, node + '.' + mreg.name().substring(THREAD_POOLS.length() + 1),
                        m != null ? m : mreg.findMetric("TotalCompletedTasksCount"));
                }
            }
        }

        /** Records a counter; a missing metric gives no result rather than a zero. */
        private void put(Counter c, String key, Metric m) {
            if (m instanceof LongMetric)
                counters.get(c).put(label(c.prefix + key), ((LongMetric)m).value());
            else if (m instanceof IntMetric)
                counters.get(c).put(label(c.prefix + key), (long)((IntMetric)m).value());
        }
    }
}
