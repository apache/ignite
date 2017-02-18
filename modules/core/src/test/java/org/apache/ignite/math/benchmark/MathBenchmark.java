package org.apache.ignite.math.benchmark;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Locale;

/** Refer {@link MathBenchmarkSelfTest} for usage examples.*/
class MathBenchmark {
    /** */
    private final String outputFileName;

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
    private MathBenchmark(String benchmarkName, String outputFileName, int measurementTimes, int warmupTimes,
        String tag, String comments) {
        this.benchmarkName = benchmarkName;
        this.outputFileName = outputFileName;
        this.measurementTimes = measurementTimes;
        this.warmUpTimes = warmupTimes;
        this.tag = tag;
        this.comments = comments;
        validate();
    }

    /**
     * Benchmark with specified name and default parameters. In particular, default output file
     * is "src/test/resources/math.benchmark.results.csv".
     * @param benchmarkName name
     */
    MathBenchmark(String benchmarkName) {
        this(benchmarkName, "src/test/resources/math.benchmark.results.csv", 100, 1, "", "");
    }

    /**
     * Executes the code using config of this benchmark.
     * @param code code to execute
     * @throws Exception if something goes wrong
     */
    void execute(BenchmarkCode code) throws Exception {
        for (int cnt = 0; cnt < warmUpTimes; cnt++)
            code.call();

        final long start = System.currentTimeMillis();

        for (int cnt = 0; cnt < measurementTimes; cnt++)
            code.call();

        final long end = System.currentTimeMillis();

        writeResults(formatResults(start, end));
    }

    /**
     * Set optional output file name, null for using stdout.
     * @param param name
     * @return configured benchmark
     */
    MathBenchmark outputFileName(String param) {
        return new MathBenchmark(benchmarkName, param, measurementTimes, warmUpTimes, tag, comments);
    }

    /**
     * Set optional measurement times.
     * @param param times
     * @return configured benchmark
     */
    MathBenchmark measurementTimes(int param) {
        return new MathBenchmark(benchmarkName, outputFileName, param, warmUpTimes, tag, comments);
    }

    /**
     * Set optional warm up times.
     * @param param times
     * @return configured benchmark
     */
    MathBenchmark warmUpTimes(int param) {
        return new MathBenchmark(benchmarkName, outputFileName, measurementTimes, param, tag, comments);
    }

    /**
     * Set optional tag to help filtering specific kind of benchmark results.
     * @param param name
     * @return configured benchmark
     */
    MathBenchmark tag(String param) {
        return new MathBenchmark(benchmarkName, outputFileName, measurementTimes, warmUpTimes, param, comments);
    }

    /**
     * Set optional comments.
     * @param param name
     * @return configured benchmark
     */
    MathBenchmark comments(String param) {
        return new MathBenchmark(benchmarkName, outputFileName, measurementTimes, warmUpTimes, tag, param);
    }

    /** */
    private void writeResults(String results) throws IOException {
        if (outputFileName == null) {
            System.out.println(results);

            return;
        }

        final String unixLineSeparator = "\n";

        try (final PrintWriter writer = new PrintWriter(Files.newBufferedWriter(Paths.get(outputFileName),
            StandardOpenOption.APPEND, StandardOpenOption.CREATE))) {
            writer.write(results + unixLineSeparator);
        }
    }

    /** */
    private String formatResults(long start, long end) {
        final String delim = ",";

        assert !formatDouble(1000_000_001.1).contains(delim) : "Formatted results contain [" + delim + "].";

        return benchmarkName +
            delim +
            start + // IMPL NOTE timestamp
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
    private void validate() {
        if (benchmarkName == null || benchmarkName.isEmpty())
            throw new IllegalArgumentException("Invalid benchmark name: [" + benchmarkName + "].");

        if (measurementTimes < 1)
            throw new IllegalArgumentException("Invalid measurement times: [" + measurementTimes + "].");
    }

    /** */
    interface BenchmarkCode {
        // todo find out why Callable<Void> failed to work here
        /** */ void call() throws Exception;
    }
}
