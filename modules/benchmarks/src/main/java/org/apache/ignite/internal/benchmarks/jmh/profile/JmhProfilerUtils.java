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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.openjdk.jmh.profile.ProfilerException;
import org.openjdk.jmh.results.AggregationPolicy;
import org.openjdk.jmh.results.ScalarResult;

/** Thread groups, results and the run description shared by the allocation profilers. */
final class JmhProfilerUtils {
    /** Group of the benchmark threads, which call the Ignite API. */
    private static final String BENCHMARK = "benchmark";

    /** System properties written to the run description. */
    private static final String[] RUN_PROPS = {"java.vendor", "java.vm.vendor", "java.runtime.version", "os.name",
        "os.arch"};

    /** */
    private JmhProfilerUtils() {
        // No-op.
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
    static String group(String name, Map<String, Boolean> clients) {
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
    static String label(String s) {
        return s.replaceAll("[^A-Za-z0-9._:%-]", "_");
    }

    /** @return Result of a value per operation. */
    static ScalarResult result(String lbl, long val, long ops, String unit) {
        return new ScalarResult(lbl, (double)val / ops, unit, AggregationPolicy.AVG);
    }

    /** @return Client flag per node name, for the named nodes of this JVM. */
    static Map<String, Boolean> clients() {
        Map<String, Boolean> clients = new HashMap<>();

        for (Ignite ignite : Ignition.allGrids()) {
            if (ignite.name() != null)
                clients.put(ignite.name(), ignite.cluster().localNode().isClient());
        }

        return clients;
    }

    /**
     * Writes the JDK and OS of the run to {@code <dir>/run.properties}.
     *
     * @param dir Directory.
     * @throws ProfilerException If the file can't be written.
     */
    static void writeRunDescription(Path dir) throws ProfilerException {
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
}
