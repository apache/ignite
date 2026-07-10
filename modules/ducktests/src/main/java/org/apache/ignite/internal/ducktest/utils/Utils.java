package org.apache.ignite.internal.ducktest.utils;

import java.util.Locale;
import java.util.function.Supplier;

/** */
public final class Utils {
    /**
     * Runs the operation, records its latency into {@code stats} and returns its result.
     * If the operation throws, nothing is recorded and the exception propagates.
     */
    public static <T> T timed(OpStats stats, Supplier<T> op) {
        long startNs = System.nanoTime();

        T val = op.get();

        stats.record(System.nanoTime() - startNs);

        return val;
    }

    /**
     * Runs the void operation and records its latency into {@code stats}.
     * If the operation throws, nothing is recorded and the exception propagates.
     */
    public static void timed(OpStats stats, Runnable op) {
        timed(stats, () -> {
            op.run();

            return null;
        });
    }

    /** Formats a nanosecond latency as milliseconds with 3 decimal places. */
    public static String fmtMs(double ns) {
        return String.format(Locale.US, "%.3f", ns < 0 ? -1.0 : ns / 1e6);
    }
}
