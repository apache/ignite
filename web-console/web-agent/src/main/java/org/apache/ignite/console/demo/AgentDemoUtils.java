

package org.apache.ignite.console.demo;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Utilities for Agent demo mode.
 */
public class AgentDemoUtils {
    /** Counter for threads in pool. */
    private static final AtomicInteger THREAD_CNT = new AtomicInteger(0);

    /**
     * Creates a thread pool that can schedule commands to run after a given delay, or to execute periodically.
     *
     * @param corePoolSize Number of threads to keep in the pool, even if they are idle.
     * @param threadName Part of thread name that would be used by thread factory.
     * @return Newly created scheduled thread pool.
     */
    public static ScheduledExecutorService newScheduledThreadPool(int corePoolSize, final String threadName) {
        ScheduledExecutorService srvc = Executors.newScheduledThreadPool(corePoolSize, new ThreadFactory() {
            @Override public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, String.format("%s-%d", threadName, THREAD_CNT.getAndIncrement()));

                thread.setDaemon(true);

                return thread;
            }
        });

        ScheduledThreadPoolExecutor executor = (ScheduledThreadPoolExecutor)srvc;

        // Setting up shutdown policy.
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);

        return srvc;
    }

    /**
     * Round value.
     *
     * @param val Value to round.
     * @param places Numbers after point.
     * @return Rounded value;
     */
    public static double round(double val, int places) {
        if (places < 0)
            throw new IllegalArgumentException();

        long factor = (long)Math.pow(10, places);

        val *= factor;

        long tmp = Math.round(val);

        return (double)tmp / factor;
    }
}
