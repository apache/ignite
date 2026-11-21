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
import java.util.Map;
import java.util.Properties;
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

    /** @return Group of a thread by name: {@code server.node0.sys-stripe} for {@code sys-stripe-3-#12%node0%}. */
    static String group(String name, Map<String, Boolean> clients) {
        String node = nodeOf(name);

        if (node == null)
            return label(name.contains("jmh-worker") ? BENCHMARK : "other." + name.replaceAll("-?\\d+$", ""));

        String pool = poolOf(name);
        Boolean client = clients.get(node);

        if (client == null)
            return label(name.contains("thin-client") ? "thin." + pool : "other." + node + '.' + pool);

        return label((client ? "client." : "server.") + node + '.' + pool);
    }

    /** @return Node from the last {@code %...%} pair ({@code wal-file-archiver%node0-#5%node0%}), or {@code null}. */
    private static String nodeOf(String name) {
        int last = name.lastIndexOf('%');
        int prev = last > 0 ? name.lastIndexOf('%', last - 1) : -1;

        return prev >= 0 && last == name.length() - 1 ? name.substring(prev + 1, last) : null;
    }

    /** @return Pool of a node's thread: the name before {@code -#} or {@code %}, without {@code -[...]} and index. */
    private static String poolOf(String name) {
        int end = name.indexOf('%');
        int hash = name.indexOf("-#");

        if (hash >= 0 && hash < end)
            end = hash;

        return name.substring(0, end).replaceAll("-\\[.*?]", "").replaceAll("-\\d+$", "");
    }

    /** @return Label with characters outside {@code [A-Za-z0-9._:%-]} replaced: JMH's JSON breaks lines at brackets. */
    static String label(String s) {
        return s.replaceAll("[^A-Za-z0-9._:%-]", "_");
    }

    /** @return Result of a value per operation. */
    static ScalarResult result(String lbl, long val, long ops, String unit) {
        return new ScalarResult(lbl, (double)val / ops, unit, AggregationPolicy.AVG);
    }

    /** Writes the JDK and OS of the run to {@code <dir>/run.properties}. */
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
