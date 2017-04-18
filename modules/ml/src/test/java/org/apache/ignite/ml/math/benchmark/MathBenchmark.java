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

package org.apache.ignite.ml.math.benchmark;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

/** Refer {@link MathBenchmarkSelfTest} for usage examples. */
class MathBenchmark {
    /** */
    private final boolean outputToConsole;

    /** */
    private final String benchmarkName;

    /** */
    private final int measurementTimes;

    /** */
    private final int warmUpTimes;

    /** */
    private final String tag;

    /** */
    private final String comments;

    /** Constructor strictly for use within this class. */
    private MathBenchmark(String benchmarkName, boolean outputToConsole, int measurementTimes, int warmUpTimes,
        String tag, String comments) {
        this.benchmarkName = benchmarkName;
        this.outputToConsole = outputToConsole;
        this.measurementTimes = measurementTimes;
        this.warmUpTimes = warmUpTimes;
        this.tag = tag;
        this.comments = comments;
        validate();
    }

    /**
     * Benchmark with specified name and default parameters, in particular, default output file.
     *
     * @param benchmarkName name
     */
    MathBenchmark(String benchmarkName) {
        this(benchmarkName, false, 100, 1, "", "");
    }

    /**
     * Executes the code using config of this benchmark.
     *
     * @param code code to execute
     * @throws Exception if something goes wrong
     */
    void execute(BenchmarkCode code) throws Exception {
        System.out.println("Started benchmark [" + benchmarkName + "].");

        for (int cnt = 0; cnt < warmUpTimes; cnt++)
            code.call();

        final long start = System.currentTimeMillis();

        for (int cnt = 0; cnt < measurementTimes; cnt++)
            code.call();

        final long end = System.currentTimeMillis();

        writeResults(formatResults(start, end));

        System.out.println("Finished benchmark [" + benchmarkName + "].");
    }

    /**
     * Set optional output mode for using stdout.
     *
     * @return configured benchmark
     */
    MathBenchmark outputToConsole() {
        return new MathBenchmark(benchmarkName, true, measurementTimes, warmUpTimes, tag, comments);
    }

    /**
     * Set optional measurement times.
     *
     * @param param times
     * @return configured benchmark
     */
    MathBenchmark measurementTimes(int param) {
        return new MathBenchmark(benchmarkName, outputToConsole, param, warmUpTimes, tag, comments);
    }

    /**
     * Set optional warm-up times.
     *
     * @param param times
     * @return configured benchmark
     */
    MathBenchmark warmUpTimes(int param) {
        return new MathBenchmark(benchmarkName, outputToConsole, measurementTimes, param, tag, comments);
    }

    /**
     * Set optional tag to help filtering specific kind of benchmark results.
     *
     * @param param name
     * @return configured benchmark
     */
    MathBenchmark tag(String param) {
        return new MathBenchmark(benchmarkName, outputToConsole, measurementTimes, warmUpTimes, param, comments);
    }

    /**
     * Set optional comments.
     *
     * @param param name
     * @return configured benchmark
     */
    MathBenchmark comments(String param) {
        return new MathBenchmark(benchmarkName, outputToConsole, measurementTimes, warmUpTimes, tag, param);
    }

    /** */
    private void writeResults(String results) throws Exception {
        if (outputToConsole) {
            System.out.println(results);

            return;
        }

        new ResultsWriter().append(results);
    }

    /** */
    private String formatResults(long start, long end) {
        final String delim = ",";

        assert !formatDouble(1000_000_001.1).contains(delim) : "Formatted results contain [" + delim + "].";

        final String ts = formatTs(start);

        assert !ts.contains(delim) : "Formatted timestamp contains [" + delim + "].";

        return benchmarkName +
            delim +
            ts + // IMPL NOTE timestamp
            delim +
            formatDouble((double)(end - start) / measurementTimes) +
            delim +
            measurementTimes +
            delim +
            warmUpTimes +
            delim +
            tag +
            delim +
            comments;
    }

    /** */
    private String formatDouble(double val) {
        return String.format(Locale.US, "%f", val);
    }

    /** */
    private String formatTs(long ts) {
        final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");

        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

        return sdf.format(new Date(ts));
    }

    /** */
    private void validate() {
        if (benchmarkName == null || benchmarkName.isEmpty())
            throw new IllegalArgumentException("Invalid benchmark name: [" + benchmarkName + "].");

        if (measurementTimes < 1)
            throw new IllegalArgumentException("Invalid measurement times: [" + measurementTimes + "].");
    }

    /** */
    interface BenchmarkCode {
        // todo find out why Callable<Void> failed to work here

        /** */
        void call() throws Exception;
    }
}
