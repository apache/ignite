/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.node;

import java.io.*;

/**
 * Data transfer object for streamer configuration properties.
 */
public class VisorStreamerConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Streamer name. */
    private final String name;

    /** Events router. */
    private final String router;

    /** Flag indicating whether event should be processed at least once. */
    private final boolean atLeastOnce;

    /** Maximum number of failover attempts to try. */
    private final int maxFailoverAttempts;

    /** Maximum number of concurrent events to be processed. */
    private final int maxConcurrentSessions;

    /** Flag indicating whether streamer executor service should be shut down on GridGain stop. */
    private final boolean executorServiceShutdown;

    /** Create data transfer object with given parameters. */
    public VisorStreamerConfig(
        String name,
        String router,
        boolean atLeastOnce,
        int maxFailoverAttempts,
        int maxConcurrentSessions,
        boolean executorServiceShutdown
    ) {
        this.name = name;
        this.router = router;
        this.atLeastOnce = atLeastOnce;
        this.maxFailoverAttempts = maxFailoverAttempts;
        this.maxConcurrentSessions = maxConcurrentSessions;
        this.executorServiceShutdown = executorServiceShutdown;
    }

    /**
     * @return Streamer name.
     */
    public String name() {
        return name;
    }

    /**
     * @return Events router.
     */
    public String router() {
        return router;
    }

    /**
     * @return Flag indicating whether event should be processed at least once.
     */
    public boolean atLeastOnce() {
        return atLeastOnce;
    }

    /**
     * @return Maximum number of failover attempts to try.
     */
    public int maxFailoverAttempts() {
        return maxFailoverAttempts;
    }

    /**
     * @return Maximum number of concurrent events to be processed.
     */
    public int maxConcurrentSessions() {
        return maxConcurrentSessions;
    }

    /**
     * @return Flag indicating whether streamer executor service should be shut down on GridGain stop.
     */
    public boolean executorServiceShutdown() {
        return executorServiceShutdown;
    }
}
