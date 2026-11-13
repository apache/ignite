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

import java.io.IOException;
import java.io.Writer;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
import org.openjdk.jmh.results.AggregationPolicy;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.ScalarResult;

import static org.apache.ignite.internal.managers.discovery.GridDiscoveryManager.DISCO_METRICS;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.COMMUNICATION_METRICS_GROUP_NAME;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.SENT_MESSAGES_METRIC_NAME;

/**
 * Bytes allocated per operation by the benchmark threads, the thin client and each thread pool of the Ignite nodes
 * running in the benchmark JVM, plus communication messages and bytes and discovery messages per operation of each
 * node. With {@code dir=<path>} the profiler also writes the JDK and OS of the run to {@code <path>/run.properties}.
 */
public class JmhAllocationProfiler implements InternalProfiler {
    /** Group of the benchmark threads, which call the Ignite API. */
    private static final String BENCHMARK = "benchmark";

    /** Processed discovery messages. */
    private static final String DISCO_PROCESSED_MESSAGES = "TotalProcessedMessages";

    /** System properties written to the run description. */
    private static final String[] RUN_PROPS = {"java.vendor", "java.vm.vendor", "java.runtime.version", "os.name",
        "os.arch"};

    /** */
    private static final ThreadMXBean THREADS = (ThreadMXBean)ManagementFactory.getThreadMXBean();

    /** Snapshot at the start of the iteration. */
    private Snapshot start;

    /** Snapshot taken before the benchmark stopped its nodes, if it did in this iteration. */
    private volatile Snapshot beforeStop;

    /**
     * @param initLine Profiler options.
     * @throws ProfilerException If the options are wrong.
     */
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

        // Bytes are counted from before to after the iteration: all its operations, not only the measured ones.
        long ops = res.getMetadata().getAllOps();

        if (ops == 0)
            return List.of();

        long profilerThread = Thread.currentThread().getId();

        Map<String, Long> grps = new TreeMap<>();

        for (Map.Entry<Long, Long> e : end.bytes.entrySet()) {
            if (e.getKey() == profilerThread)
                continue;

            // A thread started during the iteration allocated all its bytes in it.
            long startBytes = start.bytes.getOrDefault(e.getKey(), 0L);

            grps.merge(group(end.names.get(e.getKey()), end.clients), e.getValue() - startBytes, Long::sum);
        }

        List<Result> out = new ArrayList<>();

        long total = 0;

        for (Map.Entry<String, Long> e : grps.entrySet()) {
            total += e.getValue();

            out.add(result("alloc.norm:" + e.getKey(), e.getValue(), ops, "B/op"));
        }

        out.add(result("alloc.norm:total", total, ops, "B/op"));

        for (Map.Entry<String, Long> e : end.counters.entrySet()) {
            Long s = start.counters.get(e.getKey());
            String unit = e.getKey().startsWith("comm.bytes") ? "B/op" : "msgs/op";

            if (s != null)
                out.add(result(e.getKey(), e.getValue() - s, ops, unit));
        }

        return out;
    }

    /** {@inheritDoc} */
    @Override public String getDescription() {
        return "Bytes allocated per operation by node and thread pool, and messages per operation by node.";
    }

    /**
     * Group of a thread, from its name. Ignite names a pool thread {@code <pool>-<index>-#<id>%<node>%}, for example
     * {@code sys-stripe-3-#12%node0%}. The group keeps the node and the pool and drops the index, so the threads of a
     * pool on a node add up: {@code server.node0.sys-stripe}. Groups:
     * <ul>
     *     <li>{@code server.<node>.<pool>}, {@code client.<node>.<pool>}: a server or client node of this JVM;</li>
     *     <li>{@code thin.<pool>}: a thin client;</li>
     *     <li>{@code other.<node>.<pool>}: a node the profiler doesn't know, for example one already stopped;</li>
     *     <li>{@code benchmark}: a JMH worker thread;</li>
     *     <li>{@code other.<name>}: any other thread.</li>
     * </ul>
     * The node comes from the last {@code %...%} pair, since some names repeat it
     * ({@code wal-file-archiver%node0-#5%node0%}), and the {@code -[...]} part of discovery thread names is dropped.
     *
     * @param name Thread name.
     * @param clients Client flag per node name.
     * @return Group label.
     */
    private static String group(String name, Map<String, Boolean> clients) {
        int last = name.lastIndexOf('%');
        int prev = last > 0 ? name.lastIndexOf('%', last - 1) : -1;

        if (prev < 0 || last != name.length() - 1)
            return label(name.contains("jmh-worker") ? BENCHMARK : "other." + name.replaceAll("-?\\d+$", ""));

        String node = name.substring(prev + 1, last);

        int poolEnd = name.indexOf('%');
        int hash = name.indexOf("-#");

        if (hash >= 0 && hash < poolEnd)
            poolEnd = hash;

        String pool = name.substring(0, poolEnd).replaceAll("-\\[.*?]", "").replaceAll("-\\d+$", "");

        Boolean client = clients.get(node);

        if (client == null)
            return label(name.contains("thin-client") ? "thin." + pool : "other." + node + '.' + pool);

        return label((client ? "client." : "server.") + node + '.' + pool);
    }

    /**
     * JMH's JSON writer breaks lines at brackets, so labels keep to a safe set of characters.
     *
     * @param s Label.
     * @return Label with other characters replaced.
     */
    private static String label(String s) {
        return s.replaceAll("[^A-Za-z0-9._:%-]", "_");
    }

    /** */
    private static ScalarResult result(String lbl, long val, long ops, String unit) {
        return new ScalarResult(lbl, (double)val / ops, unit, AggregationPolicy.AVG);
    }

    /**
     * @param dir Directory.
     * @throws ProfilerException If the file can't be written.
     */
    private static void writeRunDescription(Path dir) throws ProfilerException {
        Properties props = new Properties();

        for (String key : RUN_PROPS)
            props.setProperty(key, System.getProperty(key, ""));

        try {
            Files.createDirectories(dir);

            try (Writer w = Files.newBufferedWriter(dir.resolve("run.properties"))) {
                props.store(w, "JVM and OS of the benchmark run");
            }
        }
        catch (IOException e) {
            throw new ProfilerException(e);
        }
    }

    /** Per-thread allocated bytes and per-node message counters at one moment. */
    private static class Snapshot {
        /** Thread ID to allocated bytes. */
        final Map<Long, Long> bytes = new HashMap<>();

        /** Thread ID to name. */
        final Map<Long, String> names = new HashMap<>();

        /** Result label to counter value, e.g. {@code comm.msgs.norm:node0}. */
        final Map<String, Long> counters = new HashMap<>();

        /** Node name to client flag. */
        final Map<String, Boolean> clients = new HashMap<>();

        /** @return Snapshot of the current state. */
        static Snapshot take() {
            Snapshot s = new Snapshot();

            long[] ids = THREADS.getAllThreadIds();
            long[] bytes = THREADS.getThreadAllocatedBytes(ids);
            ThreadInfo[] infos = THREADS.getThreadInfo(ids);

            for (int i = 0; i < ids.length; i++) {
                if (bytes[i] >= 0 && infos[i] != null) {
                    s.bytes.put(ids[i], bytes[i]);
                    s.names.put(ids[i], infos[i].getThreadName());
                }
            }

            for (Ignite ignite : Ignition.allGrids()) {
                String node = ignite.name();

                // An unnamed node can't be told apart in labels, so it gets no counters.
                if (node == null)
                    continue;

                GridMetricManager mmgr = ((IgniteEx)ignite).context().metric();

                ReadOnlyMetricRegistry comm = mmgr.registry(COMMUNICATION_METRICS_GROUP_NAME);

                s.put("comm.msgs.norm:" + node, comm.findMetric(SENT_MESSAGES_METRIC_NAME));
                s.put("comm.bytes.norm:" + node, comm.findMetric(GridNioServer.SENT_BYTES_METRIC_NAME));
                s.put("disco.msgs.norm:" + node, mmgr.registry(DISCO_METRICS).findMetric(DISCO_PROCESSED_MESSAGES));

                s.clients.put(node, ignite.cluster().localNode().isClient());
            }

            return s;
        }

        /** Records a counter; a missing one gives no result rather than a zero. */
        private void put(String lbl, Metric m) {
            if (m instanceof LongMetric)
                counters.put(label(lbl), ((LongMetric)m).value());
            else if (m instanceof IntMetric)
                counters.put(label(lbl), (long)((IntMetric)m).value());
        }
    }
}
