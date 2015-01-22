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

package org.apache.ignite.streamer;

import org.apache.ignite.internal.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Streamer configuration.
 */
public class StreamerConfiguration {
    /** By default maximum number of concurrent sessions is unlimited. */
    public static final int DFLT_MAX_CONCURRENT_SESSIONS = -1;

    /** Default value for maximum failover attempts. */
    public static final int DFLT_MAX_FAILOVER_ATTEMPTS = 3;

    /** Name. */
    private String name;

    /** Window. */
    private Collection<StreamerWindow> win;

    /** Router. */
    private StreamerEventRouter router;

    /** Stages. */
    @GridToStringInclude
    private Collection<StreamerStage> stages;

    /** At least once flag. */
    private boolean atLeastOnce;

    /** Maximum number of failover attempts. */
    private int maxFailoverAttempts = DFLT_MAX_FAILOVER_ATTEMPTS;

    /** Maximum number of concurrent sessions to be processed. */
    private int maxConcurrentSessions = DFLT_MAX_CONCURRENT_SESSIONS;

    /** Streamer executor service. */
    private ExecutorService execSvc;

    /** Executor service shutdown flag. */
    private boolean execSvcShutdown;

    /**
     *
     */
    public StreamerConfiguration() {
        // No-op.
    }

    /**
     * @param c Configuration to copy.
     */
    public StreamerConfiguration(StreamerConfiguration c) {
        atLeastOnce = c.isAtLeastOnce();
        execSvc = c.getExecutorService();
        execSvcShutdown = c.isExecutorServiceShutdown();
        maxConcurrentSessions = c.getMaximumConcurrentSessions();
        maxFailoverAttempts = c.getMaximumFailoverAttempts();
        name = c.getName();
        router = c.getRouter();
        stages = c.getStages();
        win = c.getWindows();
    }

    /**
     * Gets streamer name. Must be unique within grid.
     *
     * @return Streamer name, if {@code null} then default streamer is returned.
     */
    @Nullable public String getName() {
        return name;
    }

    /**
     * Sets the name of the streamer.
     *
     * @param name Name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Gets streamer event router.
     *
     * @return Event router, if {@code null} then events will be executed locally.
     */
    @SuppressWarnings("unchecked")
    @Nullable public StreamerEventRouter getRouter() {
        return router;
    }

    /**
     * Sets router for streamer.
     *
     * @param router Router.
     */
    public void setRouter(StreamerEventRouter router) {
        this.router = router;
    }

    /**
     * Gets collection of streamer event windows. At least one window should be configured. Each window
     * must have unique name.
     *
     * @return Streamer windows.
     */
    public Collection<StreamerWindow> getWindows() {
        return win;
    }

    /**
     * Sets collection of streamer windows.
     *
     * @param win Window.
     */
    public void setWindows(Collection<StreamerWindow> win) {
        this.win = win;
    }

    /**
     * Gets collection of streamer stages. Streamer must have at least one stage to execute. Each stage
     * must have unique name.
     *
     * @return Collection of streamer stages.
     */
    public Collection<StreamerStage> getStages() {
        return stages;
    }

    /**
     * Sets stages.
     *
     * @param stages Stages.
     */
    public void setStages(Collection<StreamerStage> stages) {
        this.stages = stages;
    }

    /**
     * Gets flag indicating whether streamer should track event execution sessions and failover event execution
     * if any failure detected or any node on which execution happened has left the grid before successful response
     * is received.
     * <p>
     * Setting this flag to {@code true} will guarantee that all pipeline stages will be executed at least once for
     * each group of event submitted to streamer (or failure listener will be notified if failover cannot succeed).
     * However, it does not guarantee that each stage will be executed at most once.
     *
     * @return {@code True} if event should be processed at least once,
     *      or {@code false} if failures can be safely ignored.
     */
    public boolean isAtLeastOnce() {
        return atLeastOnce;
    }

    /**
     * @param atLeastOnce {@code True} to guarantee that event will be processed at least once.
     */
    public void setAtLeastOnce(boolean atLeastOnce) {
        this.atLeastOnce = atLeastOnce;
    }

    /**
     * Gets maximum number of failover attempts to try when pipeline execution has failed. This parameter
     * is ignored if {@link #isAtLeastOnce()} is set to {@code false}.
     * <p>
     * If not set, default value is
     *
     * @return Maximum number of failover attempts to try.
     */
    public int getMaximumFailoverAttempts() {
        return maxFailoverAttempts;
    }

    /**
     * Sets maximum number of failover attempts.

     * @param maxFailoverAttempts Maximum number of failover attempts.
     * @see #getMaximumFailoverAttempts()
     */
    public void setMaximumFailoverAttempts(int maxFailoverAttempts) {
        this.maxFailoverAttempts = maxFailoverAttempts;
    }

    /**
     * Gets maximum number of concurrent events to be processed by streamer. This property is taken into
     * account when {@link #isAtLeastOnce()} is set to {@code true}. If not positive, number of sessions
     * will not be limited by any value.
     *
     * @return Maximum number of concurrent events to be processed. If number of concurrent events is greater
     *      then this value, caller will be blocked until enough responses are received.
     */
    public int getMaximumConcurrentSessions() {
        return maxConcurrentSessions;
    }

    /**
     * Sets maximum number of concurrent sessions.
     *
     * @param maxConcurrentSessions Maximum number of concurrent sessions.
     * @see #getMaximumConcurrentSessions()
     */
    public void setMaximumConcurrentSessions(int maxConcurrentSessions) {
        this.maxConcurrentSessions = maxConcurrentSessions;
    }

    /**
     * Gets streamer executor service. Defines a thread pool in which streamer stages will be executed.
     * <p>
     * If not specified, thread pool executor with max pool size equal to number of cores will be created.
     *
     * @return Streamer executor service.
     */
    public ExecutorService getExecutorService() {
        return execSvc;
    }

    /**
     * Sets streamer executor service.
     *
     * @param execSvc Executor service to use.
     * @see #getExecutorService()
     */
    public void setExecutorService(ExecutorService execSvc) {
        this.execSvc = execSvc;
    }

    /**
     * Flag indicating whether streamer executor service should be shut down on GridGain stop. If this flag
     * is {@code true}, executor service will be shut down regardless of whether executor was specified externally
     * or it was created by GridGain.
     *
     * @return {@code True} if executor service should be shut down on GridGain stop.
     */
    public boolean isExecutorServiceShutdown() {
        return execSvcShutdown;
    }

    /**
     * Sets flag indicating whether executor service should be shut down on GridGain stop.
     *
     * @param execSvcShutdown {@code True} if executor service should be shut down on GridGain stop.
     * @see #isExecutorServiceShutdown()
     */
    public void setExecutorServiceShutdown(boolean execSvcShutdown) {
        this.execSvcShutdown = execSvcShutdown;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StreamerConfiguration.class, this);
    }
}
