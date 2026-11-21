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
import java.lang.reflect.Field;
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
import jdk.jfr.consumer.RecordedFrame;
import jdk.jfr.consumer.RecordedMethod;
import jdk.jfr.consumer.RecordedStackTrace;
import jdk.jfr.consumer.RecordedThread;
import jdk.jfr.consumer.RecordingFile;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.profile.ProfilerException;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.runner.IterationType;

import static java.nio.file.StandardOpenOption.APPEND;
import static org.apache.ignite.internal.benchmarks.jmh.profile.JmhProfilerUtils.group;
import static org.apache.ignite.internal.benchmarks.jmh.profile.JmhProfilerUtils.result;
import static org.apache.ignite.internal.benchmarks.jmh.profile.JmhProfilerUtils.writeRunDescription;

/** Objects and bytes per operation by class, node and thread pool, recorded with JFR; needs {@code -XX:-UseTLAB}. */
public class JmhAllocationHistogramProfiler implements InternalProfiler {
    /** JFR event of an allocation outside a TLAB: with TLABs off, of every allocation. */
    private static final String ALLOC_EVENT = "jdk.ObjectAllocationOutsideTLAB";

    /** JFR event of lost recording data. */
    private static final String DATA_LOSS_EVENT = "jdk.DataLoss";

    /** Primitive array element types by their descriptor code. */
    private static final Map<Character, String> PRIMITIVES = Map.of('Z', "boolean", 'B', "byte", 'C', "char", 'S',
        "short", 'I', "int", 'J', "long", 'F', "float", 'D', "double");

    /** Output directory. */
    private final Path dir;

    /** Stack frames written per allocation, or 0 for none. */
    private final int frameCnt;

    /** Per-class file of this benchmark, parameter set and fork. */
    private Path file;

    /** Recording of the current iteration. */
    private Recording rec;

    /** Whether the current recording has stack traces. */
    private boolean stacks;

    /** Client flag per node name, read when the recording stops. */
    private Map<String, Boolean> clients;

    /** Names of the threads alive at the start of the iteration. */
    private List<String> liveThreads;

    /** Whether named nodes ran at the start of the iteration. */
    private boolean nodesAtStart;

    /** Warmup iterations so far. */
    private int warmups;

    /** Measurement iterations so far. */
    private int measurements;

    /** @param initLine {@code dir=<path>} for the per-class tables, optionally with {@code ;site=<frames>}. */
    public JmhAllocationHistogramProfiler(String initLine) throws ProfilerException {
        Path dir = null;
        int frameCnt = 0;

        for (String opt : initLine.split(";")) {
            if (opt.startsWith("dir="))
                dir = Paths.get(opt.substring("dir=".length()));
            else if (opt.startsWith("site="))
                frameCnt = siteFrames(opt);
            else if (!opt.isEmpty())
                throw new ProfilerException("Unknown option, expected dir=<path>;site=<frames>: " + opt);
        }

        if (dir == null)
            throw new ProfilerException("Expected dir=<path>: " + initLine);

        this.dir = dir;
        this.frameCnt = frameCnt;

        writeRunDescription(dir);

        JmhNodeStopHook.set(JmhAllocationHistogramProfiler.class, this::stop);
    }

    /** {@inheritDoc} */
    @Override public void beforeIteration(BenchmarkParams benchParams, IterationParams iterParams) {
        if (file == null)
            init(benchParams);

        liveThreads = threadNames();
        nodesAtStart = !clients().isEmpty();

        startRecording(iterParams);
    }

    /** {@inheritDoc} */
    @Override public Collection<? extends Result> afterIteration(
        BenchmarkParams benchParams,
        IterationParams iterParams,
        IterationResult res
    ) {
        stop();

        if (nodesAtStart && clients.isEmpty())
            throw new IllegalStateException(JmhNodeStopHook.NOT_CALLED);

        Map<String, Map<String, long[]>> hist = histogram();

        long ops = res.getMetadata().getAllOps();

        if (ops == 0)
            return List.of();

        String iter = iterParams.getType() == IterationType.WARMUP
            ? "warmup-" + ++warmups
            : String.valueOf(++measurements);

        writeTable(hist, iter, ops);

        return results(hist, ops);
    }

    /** {@inheritDoc} */
    @Override public String getDescription() {
        return "Objects and bytes allocated per operation for each class, by node and thread pool. Needs -XX:-UseTLAB.";
    }

    /** Checks that TLABs are off and names the file; not in the constructor, which JMH also runs in its own JVM. */
    private void init(BenchmarkParams benchParams) {
        HotSpotDiagnosticMXBean hotspot = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);

        if (Boolean.parseBoolean(hotspot.getVMOption("UseTLAB").getValue())) {
            throw new IllegalStateException(
                "Run the benchmark with -XX:-UseTLAB: the profiler counts only allocations outside TLABs.");
        }

        file = dir.resolve(fileName(benchParams));
    }

    /** @return Names of the live threads other than the current one. */
    private static List<String> threadNames() {
        List<String> names = new ArrayList<>();

        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (t != Thread.currentThread())
                names.add(t.getName());
        }

        return names;
    }

    /** Starts recording allocations, with stacks in measurement iterations if {@code site} is set. */
    private void startRecording(IterationParams iterParams) {
        rec = new Recording();

        // Warmup records no stacks: during node start, stacks make JFR drop events or the fork run out of heap.
        stacks = frameCnt > 0 && iterParams.getType() == IterationType.MEASUREMENT;

        if (stacks)
            rec.enable(ALLOC_EVENT).withStackTrace();
        else
            rec.enable(ALLOC_EVENT).withoutStackTrace();

        rec.enable(DATA_LOSS_EVENT);

        rec.start();
    }

    /** Stops the recording and reads the client flags of the nodes, which may have started during the iteration. */
    private void stop() {
        if (rec != null && rec.getState() == RecordingState.RUNNING) {
            rec.stop();

            clients = clients();
        }
    }

    /** @return Client flag per node name, for the named nodes of this JVM. */
    private static Map<String, Boolean> clients() {
        Map<String, Boolean> clients = new HashMap<>();

        for (Ignite ignite : Ignition.allGrids()) {
            if (ignite.name() != null)
                clients.put(ignite.name(), ignite.cluster().localNode().isClient());
        }

        return clients;
    }

    /** @return Group to row (the class, size and frames columns) to objects and bytes, from the recording. */
    private Map<String, Map<String, long[]>> histogram() {
        Map<String, Map<String, long[]>> hist = new TreeMap<>();

        for (String name : liveThreads)
            hist.put(group(name, clients), new TreeMap<>());

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

        return hist;
    }

    /** Appends the rows of an iteration to the per-class file. */
    private void writeTable(Map<String, Map<String, long[]>> hist, String iter, long ops) {
        boolean first = warmups + measurements == 1;

        try (Writer w = first ? Files.newBufferedWriter(file) : Files.newBufferedWriter(file, APPEND)) {
            if (first)
                w.write("group\tclass\tsize\t" + (frameCnt > 0 ? "frames\t" : "") +
                    "iteration\tobjects/op\tbytes/op\n");

            for (Map.Entry<String, Map<String, long[]>> grp : hist.entrySet()) {
                for (Map.Entry<String, long[]> row : grp.getValue().entrySet()) {
                    long[] cnt = row.getValue();

                    w.write(grp.getKey() + '\t' + row.getKey() + '\t' + iter + '\t' + (double)cnt[0] / ops + '\t' +
                        (double)cnt[1] / ops + '\n');
                }
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** @return Objects and bytes per operation of each group and of all of them. */
    private static List<Result> results(Map<String, Map<String, long[]>> hist, long ops) {
        List<Result> out = new ArrayList<>();

        long totalObjs = 0;
        long totalBytes = 0;

        for (Map.Entry<String, Map<String, long[]>> grp : hist.entrySet()) {
            long objs = 0;
            long bytes = 0;

            for (long[] cnt : grp.getValue().values()) {
                objs += cnt[0];
                bytes += cnt[1];
            }

            totalObjs += objs;
            totalBytes += bytes;

            out.add(result("jfr.objs.norm:" + grp.getKey(), objs, ops, "objs/op"));
            out.add(result("jfr.bytes.norm:" + grp.getKey(), bytes, ops, "B/op"));
        }

        out.add(result("jfr.objs.norm:total", totalObjs, ops, "objs/op"));
        out.add(result("jfr.bytes.norm:total", totalBytes, ops, "B/op"));

        return out;
    }

    /** Adds a recording's allocations, except the profiler thread's, to a histogram; fails if JFR dropped events. */
    private void read(Path jfr, Map<String, Map<String, long[]>> hist) throws IOException {
        long self = Thread.currentThread().getId();

        Map<Long, String> grps = new HashMap<>();

        // Events with the same stack share its object within a recording chunk, so each stack is formatted once.
        Map<RecordedStackTrace, String> stackTexts = new HashMap<>();

        try (RecordingFile f = new RecordingFile(jfr)) {
            while (f.hasMoreEvents()) {
                RecordedEvent ev = f.readEvent();
                String type = ev.getEventType().getName();

                if (DATA_LOSS_EVENT.equals(type)) {
                    throw new IllegalStateException("JFR dropped events, so the histogram misses allocations. Rerun " +
                        "with a larger -XX:FlightRecorderOptions:memorysize.");
                }

                if (!ALLOC_EVENT.equals(type))
                    continue;

                RecordedThread t = ev.getThread();

                if (t != null && t.getJavaThreadId() == self)
                    continue;

                String grp = t == null ? "other.unknown" : grps.computeIfAbsent(t.getJavaThreadId(),
                    id -> group(t.getJavaName(), clients));

                long[] cnt = hist.computeIfAbsent(grp, g -> new TreeMap<>())
                    .computeIfAbsent(row(ev, stackTexts), r -> new long[2]);

                cnt[0]++;
                cnt[1] += ev.getLong("allocationSize");
            }
        }
    }

    /** @return Row of an allocation: its class, an array's size range and, with {@code site}, the top frames. */
    private String row(RecordedEvent ev, Map<RecordedStackTrace, String> stackTexts) {
        RecordedClass cls = ev.getClass("objectClass");
        String name = cls == null ? "<unknown>" : cls.getName();

        String row = javaName(name) + '\t' + (name.startsWith("[") ? sizeRange(ev.getLong("allocationSize")) : "");

        if (stacks)
            return row + '\t' + stackTexts.computeIfAbsent(ev.getStackTrace(), this::frames);

        return frameCnt > 0 ? row + '\t' : row;
    }

    /** @return Top frames of a stack, allocating method first: {@code Class.method:line < ...}; empty for none. */
    private String frames(RecordedStackTrace st) {
        if (st == null)
            return "";

        StringBuilder sb = new StringBuilder();

        List<RecordedFrame> all = st.getFrames();

        for (RecordedFrame f : all.subList(0, Math.min(frameCnt, all.size()))) {
            if (sb.length() > 0)
                sb.append(" < ");

            RecordedMethod m = f.getMethod();

            if (m == null)
                sb.append("<unknown>");
            else
                sb.append(m.getType().getName()).append('.').append(m.getName());

            if (f.getLineNumber() >= 0)
                sb.append(':').append(f.getLineNumber());
        }

        return sb.toString();
    }

    /** @return Java name of a class name from JFR, such as {@code byte[]} for {@code [B}. */
    private static String javaName(String name) {
        int dims = 0;

        while (dims < name.length() && name.charAt(dims) == '[')
            dims++;

        if (dims == 0)
            return name;

        String prim = PRIMITIVES.get(name.charAt(dims));

        String elem = prim != null ? prim : name.substring(dims + 1, name.length() - 1);

        return elem + "[]".repeat(dims);
    }

    /** @return Power-of-two range of an array's size in bytes, header included: {@code <=16}, {@code <=32}, ... */
    private static String sizeRange(long size) {
        long bound = 16;

        while (bound < size)
            bound <<= 1;

        return "<=" + bound;
    }

    /** @return File name: benchmark, non-default parameters, fork PID; with every parameter, names get too long. */
    private static String fileName(BenchmarkParams benchParams) {
        String bench = benchParams.getBenchmark();
        Map<String, String> dflts = defaultParams(bench.substring(0, bench.lastIndexOf('.')));

        StringBuilder sb = new StringBuilder(bench);

        for (String key : benchParams.getParamsKeys()) {
            if (!benchParams.getParam(key).equals(dflts.get(key)))
                sb.append('-').append(key).append('=').append(benchParams.getParam(key));
        }

        return sb.toString().replaceAll("[^A-Za-z0-9._=-]", "_") + '-' + ProcessHandle.current().pid() + ".tsv";
    }

    /** @return Name to value of the {@code @Param} fields with one value, in a class and its parents. */
    private static Map<String, String> defaultParams(String cls) {
        Map<String, String> dflts = new HashMap<>();

        try {
            for (Class<?> c = Class.forName(cls); c != null; c = c.getSuperclass()) {
                for (Field f : c.getDeclaredFields()) {
                    Param p = f.getAnnotation(Param.class);

                    if (p != null && p.value().length == 1)
                        dflts.putIfAbsent(f.getName(), p.value()[0]);
                }
            }
        }
        catch (ClassNotFoundException ignored) {
            // A nested benchmark class: its name keeps every parameter.
        }

        return dflts;
    }

    /** @return Stack frames of the {@code site=<frames>} option. */
    private static int siteFrames(String opt) throws ProfilerException {
        try {
            return Integer.parseInt(opt.substring("site=".length()));
        }
        catch (NumberFormatException e) {
            throw new ProfilerException("Expected site=<frames>: " + opt);
        }
    }
}
