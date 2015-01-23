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

package org.apache.ignite.internal.visor.streamer;

import org.apache.ignite.streamer.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.*;

/**
 * Data transfer object for streamer configuration properties.
 */
public class VisorStreamerConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Streamer name. */
    private String name;

    /** Events router. */
    private String router;

    /** Flag indicating whether event should be processed at least once. */
    private boolean atLeastOnce;

    /** Maximum number of failover attempts to try. */
    private int maxFailoverAttempts;

    /** Maximum number of concurrent events to be processed. */
    private int maxConcurrentSessions;

    /** Flag indicating whether streamer executor service should be shut down on GridGain stop. */
    private boolean executorServiceShutdown;

    /**
     * @param scfg Streamer configuration.
     * @return Data transfer object for streamer configuration properties.
     */
    public static VisorStreamerConfiguration from(StreamerConfiguration scfg) {
        VisorStreamerConfiguration cfg = new VisorStreamerConfiguration();

        cfg.name(scfg.getName());
        cfg.router(compactClass(scfg.getRouter()));
        cfg.atLeastOnce(scfg.isAtLeastOnce());
        cfg.maximumFailoverAttempts(scfg.getMaximumFailoverAttempts());
        cfg.maximumConcurrentSessions(scfg.getMaximumConcurrentSessions());
        cfg.executorServiceShutdown(scfg.isExecutorServiceShutdown());

        return cfg;
    }

    /**
     * Construct data transfer object for streamer configurations properties.
     *
     * @param streamers streamer configurations.
     * @return streamer configurations properties.
     */
    public static Iterable<VisorStreamerConfiguration> list(StreamerConfiguration[] streamers) {
        if (streamers == null)
            return Collections.emptyList();

        final Collection<VisorStreamerConfiguration> cfgs = new ArrayList<>(streamers.length);

        for (StreamerConfiguration streamer : streamers)
            cfgs.add(from(streamer));

        return cfgs;
    }

    /**
     * @return Streamer name.
     */
    @Nullable public String name() {
        return name;
    }

    /**
     * @param name New streamer name.
     */
    public void name(@Nullable String name) {
        this.name = name;
    }

    /**
     * @return Events router.
     */
    @Nullable public String router() {
        return router;
    }

    /**
     * @param router New events router.
     */
    public void router(@Nullable String router) {
        this.router = router;
    }

    /**
     * @return Flag indicating whether event should be processed at least once.
     */
    public boolean atLeastOnce() {
        return atLeastOnce;
    }

    /**
     * @param atLeastOnce New flag indicating whether event should be processed at least once.
     */
    public void atLeastOnce(boolean atLeastOnce) {
        this.atLeastOnce = atLeastOnce;
    }

    /**
     * @return Maximum number of failover attempts to try.
     */
    public int maximumFailoverAttempts() {
        return maxFailoverAttempts;
    }

    /**
     * @param maxFailoverAttempts New maximum number of failover attempts to try.
     */
    public void maximumFailoverAttempts(int maxFailoverAttempts) {
        this.maxFailoverAttempts = maxFailoverAttempts;
    }

    /**
     * @return Maximum number of concurrent events to be processed.
     */
    public int maximumConcurrentSessions() {
        return maxConcurrentSessions;
    }

    /**
     * @param maxConcurrentSessions New maximum number of concurrent events to be processed.
     */
    public void maximumConcurrentSessions(int maxConcurrentSessions) {
        this.maxConcurrentSessions = maxConcurrentSessions;
    }

    /**
     * @return Flag indicating whether streamer executor service should be shut down on GridGain stop.
     */
    public boolean executorServiceShutdown() {
        return executorServiceShutdown;
    }

    /**
     * @param executorSrvcShutdown New flag indicating whether streamer executor service should be shutdown
     *      on GridGain stop.
     */
    public void executorServiceShutdown(boolean executorSrvcShutdown) {
        executorServiceShutdown = executorSrvcShutdown;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorStreamerConfiguration.class, this);
    }
}
