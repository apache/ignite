package org.apache.ignite.yardstick.cache.load;

import org.apache.ignite.IgniteCache;
import org.yardstickframework.BenchmarkUtils;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Prints a log of preloading process to the BenchmarkUtils output
 */
public class IgniteBenchmarkPreloadLogger extends Thread {

    /**
     * Map for keeping previous values to make sure all the caches work correctly.
     */
    private final Map<String, Long> counters;

    /**
     * String template used in String.format() to make output readable.
     */
    private final String formatString;


    /**
     * Time interval between printing log
     */
    private final long sleepInterval;

    /**
     * List of caches whose size to be printed during preload
     */
    private final Collection<IgniteCache<Object, Object>> caches;

    /**
     * Creates new thread which prints a number of an entries in a cache and a number of an entries loaded
     * during each time interval.
     *
     * @param caches List of available caches
     * @param logInterval Time interval in milliseconds between iterations. Required to be a positive integer.
     */
    public IgniteBenchmarkPreloadLogger(final Collection<IgniteCache<Object, Object>> caches, final long logInterval) {
        this.caches = caches;

        counters = new HashMap<>();

        int longestNameLength = 0;

        // Set up an initial values to the map
        for (IgniteCache<Object, Object> availableCache : caches) {
            counters.put(availableCache.getName(), 0L);

            //Find out the length of the longest cache name
            longestNameLength = Math.max(availableCache.getName().length(), longestNameLength);
        }

        formatString = "%-" + (longestNameLength + 4) + "s%-8d\t(+%d)";

        sleepInterval = logInterval;

        this.setName("Preloading");
    }

    /** {@inheritDoc} */
    @Override public void run() {
        while (!Thread.interrupted()) {
            for (IgniteCache<Object, Object> cache : caches) {
                String cacheName = cache.getName();

                long cacheSize = cache.sizeLong();

                long recentlyLoaded = cacheSize - counters.get(cacheName);

                String log = String.format(formatString, cacheName, cacheSize, recentlyLoaded);
                BenchmarkUtils.println(log);

                counters.put(cacheName, cacheSize);
            }

            try {
                Thread.sleep(sleepInterval);
            }
            catch (InterruptedException ignored) {
                BenchmarkUtils.println("WARNING: preload logger is interrupted.");
                return;
            }
        }

        BenchmarkUtils.println("WARNING: preload logger is interrupted.");
    }
}

