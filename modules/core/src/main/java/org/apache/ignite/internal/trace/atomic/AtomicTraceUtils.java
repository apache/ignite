package org.apache.ignite.internal.trace.atomic;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;

import java.io.File;

/**
 * Atomic trace utility methods.
 */
public class AtomicTraceUtils {
    /** Cache name. */
    public static final String CACHE_NAME = "cache";

    /** Trace directory. */
    //private static final String TRACE_DIR = System.getProperty("TRACE_DIR");
    private static final String TRACE_DIR = "C:\\Personal\\atomic_trace";

    /**
     * Get trace directory.
     *
     * @return Trace directory.
     */
    @SuppressWarnings("ConstantConditions")
    public static File traceDir() {
        if (TRACE_DIR == null || TRACE_DIR.isEmpty())
            throw new RuntimeException("TRACE_DIR is not defined.");

        return new File(TRACE_DIR);
    }

    /**
     * Get trace file with the given index.
     *
     * @param idx Index.
     * @return Trace file.
     */
    public static File traceFile(int idx) {
        return new File(traceDir(), "trace-" + idx + ".log");
    }

    /**
     * Clear trace directory.
     *
     * @return Directory.
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static File prepareTraceDir() {
        File dir = traceDir();

        if (dir.exists()) {
            if (!U.delete(dir))
                throw new RuntimeException("Failed to clear trace dir: " + dir.getAbsolutePath());
        }

        dir.mkdirs();

        return dir;
    }

    /**
     * Create configuration.
     *
     * @param name Name.
     * @param client Client flag.
     * @return Configuration.
     */
    public static IgniteConfiguration config(String name, boolean client) {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName(name);
        cfg.setClientMode(client);
        cfg.setLocalHost("127.0.0.1");

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(CACHE_NAME);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * Private constructor.
     */
    private AtomicTraceUtils() {
        // No-op.
    }
}
