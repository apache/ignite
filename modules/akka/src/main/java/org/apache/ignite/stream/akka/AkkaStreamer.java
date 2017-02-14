package org.apache.ignite.stream.akka;

import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.stream.StreamAdapter;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Akka streamer broke transmitters key-value pair messages and ingest into an {@link IgniteDataStreamer} instance.
 */
public class AkkaStreamer<T, K, V> extends StreamAdapter<T, K, V> {
    /** Default flush frequency. */
    private static final long DFLT_FLUSH_FREQ = 10000L;

    /** Retry timeout. */
    private static final long DFLT_RETRY_TIMEOUT = 10000;


    /** Logger. */
    private IgniteLogger log;

    /** Automatic flush frequency. */
    private long autoFlushFrequency = DFLT_FLUSH_FREQ;

    /** Enables overwriting existing values in cache. */
    private boolean allowOverwrite = false;

    /** Flag for stopped state. */
    private static volatile boolean stopped = true;

    /** Retry timeout. */
    private long retryTimeout = DFLT_RETRY_TIMEOUT;

    /** Executor used to submit kafka streams. */
    private ExecutorService executor;


    /** Number of threads to process kafka streams. */
    private int threads;

    private HashMap<K, V> messageMap;

    /** Akka streamer constructor. */
    public AkkaStreamer(HashMap<K,V> messageMap) {
        this.messageMap = messageMap;
    }

    /**
     * Obtains data flush frequency.
     *
     * @return Flush frequency.
     */
    public long getAutoFlushFrequency() {
        return autoFlushFrequency;
    }

    /**
     * Specifies data flush frequency into the grid.
     *
     * @param autoFlushFrequency Flush frequency.
     */
    public void setAutoFlushFrequency(long autoFlushFrequency) {
        this.autoFlushFrequency = autoFlushFrequency;
    }

    /**
     * Obtains flag for enabling overwriting existing values in cache.
     *
     * @return True if overwriting is allowed, false otherwise.
     */
    public boolean getAllowOverwrite() {
        return allowOverwrite;
    }

    /**
     * Enables overwriting existing values in cache.
     *
     * @param allowOverwrite Flag value.
     */
    public void setAllowOverwrite(boolean allowOverwrite) {
        this.allowOverwrite = allowOverwrite;
    }

    /**
     * Sets the threads.
     *
     * @param threads Number of threads.
     */
    public void setThreads(int threads) {
        this.threads = threads;
    }

    /**
     * Sets the retry timeout.
     *
     * @param retryTimeout Retry timeout.
     */
    public void setRetryTimeout(long retryTimeout) {
        A.ensure(retryTimeout > 0, "retryTimeout > 0");

        this.retryTimeout = retryTimeout;
    }


    /**
     * Starts streamer.
     *
     * @throws IgniteException If failed.
     */
    public void start() {
        A.notNull(getStreamer(), "streamer");
        A.notNull(getIgnite(), "ignite");
        A.ensure(threads > 0, "threads > 0");

        log = getIgnite().log();

        // Now launch all the consumer threads.
        executor = Executors.newFixedThreadPool(threads);

        stopped = false;

        executor.submit(new Runnable() {
            @Override public void run() {
                while ((!stopped)) {
                    try {
                        for(K key : messageMap.keySet()) {
                            try {
                                getStreamer().addData(key, messageMap.get(key));
                            }
                            catch (Exception e ) {
                                U.warn(log, "Message is ignored due to an error [msg=" + messageMap + ']', e);
                            }

                        }
                    }
                    catch (Exception e) {
                        U.warn(log, "Message can't be consumed from stream. Retry after " +
                                retryTimeout + " ms.", e);

                        try {
                            Thread.sleep(retryTimeout);
                        }
                        catch (InterruptedException ie) {
                            // No-op.
                        }
                    }
                }
            }
        });

    }

    /**
     * Stops streamer.
     */
    public void stop() {
        stopped = true;

        if (executor != null) {
            executor.shutdown();

            try {
                if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS))
                    if (log.isDebugEnabled())
                        log.debug("Timed out waiting for consumer threads to shut down, exiting uncleanly.");
            }
            catch (InterruptedException e) {
                if (log.isDebugEnabled())
                    log.debug("Interrupted during shutdown, exiting uncleanly.");
            }
        }
    }

}
