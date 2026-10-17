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
import java.io.UncheckedIOException;
import java.io.Writer;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import com.sun.management.HotSpotDiagnosticMXBean;
import jdk.jfr.Recording;
import jdk.jfr.RecordingState;
import jdk.jfr.consumer.RecordedClass;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedThread;
import jdk.jfr.consumer.RecordingFile;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.profile.ProfilerException;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.runner.IterationType;

import static java.nio.file.StandardOpenOption.APPEND;

/**
 * Objects and bytes allocated per operation for each class, by node and thread pool as in
 * {@link JmhAllocationProfiler}, recorded with JFR. Needs {@code -XX:-UseTLAB}: the profiler counts only allocations
 * outside TLABs. Writes the totals per group to the JMH results and the per-class table to {@code dir=<path>}.
 */
public class JmhAllocationHistogramProfiler implements InternalProfiler {
    /** JFR event of an allocation outside a TLAB: with TLABs off, of every allocation. */
    private static final String ALLOC_EVENT = "jdk.ObjectAllocationOutsideTLAB";

    /** JFR event of lost recording data. */
    private static final String DATA_LOSS_EVENT = "jdk.DataLoss";

    /** Output directory. */
    private final Path dir;

    /** Per-class file of this benchmark, parameter set and fork. */
    private Path file;

    /** Recording of the current iteration. */
    private Recording rec;

    /** Client flag per node name, read when the recording stops. */
    private volatile Map<String, Boolean> clients;

    /** Names of the threads alive at the start of the iteration. */
    private List<String> liveThreads;

    /** Warmup iterations so far. */
    private int warmups;

    /** Measurement iterations so far. */
    private int measurements;

    /**
     * @param initLine Profiler options.
     * @throws ProfilerException If the options are wrong.
     */
    public JmhAllocationHistogramProfiler(String initLine) throws ProfilerException {
        if (!initLine.startsWith("dir="))
            throw new ProfilerException("Expected dir=<path>: " + initLine);

        dir = Paths.get(initLine.substring("dir=".length()));

        JmhAllocationProfiler.writeRunDescription(dir);

        JmhNodeStopHook.set(JmhAllocationHistogramProfiler.class, this::stop);
    }

    /** {@inheritDoc} */
    @Override public void beforeIteration(BenchmarkParams benchParams, IterationParams iterParams) {
        if (file == null) {
            HotSpotDiagnosticMXBean hotspot = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);

            if (Boolean.parseBoolean(hotspot.getVMOption("UseTLAB").getValue())) {
                throw new IllegalStateException(
                    "Run the benchmark with -XX:-UseTLAB: the profiler counts only allocations outside TLABs.");
            }

            file = dir.resolve(fileName(benchParams));
        }

        liveThreads = new ArrayList<>();

        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (t != Thread.currentThread())
                liveThreads.add(t.getName());
        }

        rec = new Recording();

        rec.enable(ALLOC_EVENT).withoutStackTrace();
        rec.enable(DATA_LOSS_EVENT);

        rec.start();
    }

    /** {@inheritDoc} */
    @Override public Collection<? extends Result> afterIteration(
        BenchmarkParams benchParams,
        IterationParams iterParams,
        IterationResult res
    ) {
        stop();

        // Group to class to objects and bytes; a group of a live thread without allocations reports zeros.
        Map<String, Map<String, long[]>> hist = new TreeMap<>();

        for (String name : liveThreads)
            hist.put(JmhAllocationProfiler.group(name, clients), new TreeMap<>());

        try {
            Path jfr = Files.createTempFile("jmh-alloc-", ".jfr");

            try (Recording r = rec) {
                r.dump(jfr);

                read(jfr, hist);
            }
            finally {
                Files.delete(jfr);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        long ops = res.getMetadata().getAllOps();

        if (ops == 0)
            return List.of();

        String iter = iterParams.getType() == IterationType.WARMUP
            ? "warmup-" + ++warmups
            : String.valueOf(++measurements);

        List<Result> out = new ArrayList<>();

        long totalObjs = 0;
        long totalBytes = 0;

        boolean first = warmups + measurements == 1;

        // The first iteration of a fork replaces a file left by an earlier run with the same process ID.
        try (Writer w = first ? Files.newBufferedWriter(file) : Files.newBufferedWriter(file, APPEND)) {
            if (first)
                w.write("group\tclass\titeration\tobjects/op\tbytes/op\n");

            for (Map.Entry<String, Map<String, long[]>> grp : hist.entrySet()) {
                long objs = 0;
                long bytes = 0;

                for (Map.Entry<String, long[]> cls : grp.getValue().entrySet()) {
                    long[] cnt = cls.getValue();

                    objs += cnt[0];
                    bytes += cnt[1];

                    w.write(grp.getKey() + '\t' + cls.getKey() + '\t' + iter + '\t' + (double)cnt[0] / ops + '\t' +
                        (double)cnt[1] / ops + '\n');
                }

                totalObjs += objs;
                totalBytes += bytes;

                out.add(JmhAllocationProfiler.result("jfr.objs.norm:" + grp.getKey(), objs, ops, "objs/op"));
                out.add(JmhAllocationProfiler.result("jfr.bytes.norm:" + grp.getKey(), bytes, ops, "B/op"));
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        out.add(JmhAllocationProfiler.result("jfr.objs.norm:total", totalObjs, ops, "objs/op"));
        out.add(JmhAllocationProfiler.result("jfr.bytes.norm:total", totalBytes, ops, "B/op"));

        return out;
    }

    /** {@inheritDoc} */
    @Override public String getDescription() {
        return "Objects and bytes allocated per operation for each class, by node and thread pool. Needs -XX:-UseTLAB.";
    }

    /**
     * Stops the recording and reads the client flags of the nodes, which start only after the first iteration has
     * begun. Runs before the nodes stop in the last iteration and at the end of the others.
     */
    private void stop() {
        if (rec != null && rec.getState() == RecordingState.RUNNING) {
            rec.stop();

            clients = JmhAllocationProfiler.clients();
        }
    }

    /**
     * Adds the allocations of a recording to a histogram, except those of the profiler's own thread.
     *
     * @param jfr Recording file.
     * @param hist Group to class to objects and bytes.
     * @throws IOException If the file can't be read.
     * @throws IllegalStateException If JFR dropped events.
     */
    private void read(Path jfr, Map<String, Map<String, long[]>> hist) throws IOException {
        long self = Thread.currentThread().getId();

        Map<Long, String> grps = new HashMap<>();

        try (RecordingFile f = new RecordingFile(jfr)) {
            while (f.hasMoreEvents()) {
                RecordedEvent ev = f.readEvent();
                String type = ev.getEventType().getName();

                if (DATA_LOSS_EVENT.equals(type)) {
                    throw new IllegalStateException("JFR dropped events, so the histogram misses allocations. Rerun " +
                        "with a larger -XX:FlightRecorderOptions:memorysize.");
                }

                // The file also holds the events of other recordings running in this JVM.
                if (!ALLOC_EVENT.equals(type))
                    continue;

                // JFR sometimes records an allocation without its thread or class.
                RecordedThread t = ev.getThread();
                RecordedClass cls = ev.getClass("objectClass");

                if (t != null && t.getJavaThreadId() == self)
                    continue;

                String grp = t == null ? "other.unknown" : grps.computeIfAbsent(t.getJavaThreadId(),
                    id -> JmhAllocationProfiler.group(t.getJavaName(), clients));

                long[] cnt = hist.computeIfAbsent(grp, g -> new TreeMap<>())
                    .computeIfAbsent(cls == null ? "<unknown>" : cls.getName(), c -> new long[2]);

                cnt[0]++;
                cnt[1] += ev.getLong("allocationSize");
            }
        }
    }

    /**
     * @param benchParams Benchmark parameters.
     * @return File name with the benchmark, its parameters and the process ID of the fork.
     */
    private static String fileName(BenchmarkParams benchParams) {
        StringBuilder sb = new StringBuilder(benchParams.getBenchmark());

        for (String key : benchParams.getParamsKeys())
            sb.append('-').append(key).append('=').append(benchParams.getParam(key));

        return sb.toString().replaceAll("[^A-Za-z0-9._=-]", "_") + '-' + ProcessHandle.current().pid() + ".tsv";
    }
}
