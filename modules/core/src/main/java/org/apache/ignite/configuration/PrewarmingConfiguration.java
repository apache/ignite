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
package org.apache.ignite.configuration;

import java.io.Serializable;

/**
 * This class defines page memory prewarming configuration.
 */
public class PrewarmingConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Runtime dump disabled. */
    public static final long RUNTIME_DUMP_DISABLED = -1;

    /**
     * Optimal count of threads for warm up pages loading into memory.
     * That value was obtained through testing warm up functionality on 28 cores with hyperThreading.
     */
    public static final int OPTIMAL_PAGE_LOAD_THREADS = 16;

    /**
     * Optimal count of threads for warm up dump files reading.
     * That value was obtained through testing warm up functionality on 28 cores with hyperThreading.
     */
    public static final int OPTIMAL_DUMP_READ_THREADS = 4;

    /** Prewarming of indexes only flag. */
    private boolean indexesOnly;

    /** Wait prewarming on start flag. */
    private boolean waitPrewarmingOnStart;

    /** Prewarming runtime dump delay. */
    private long runtimeDumpDelay = RUNTIME_DUMP_DISABLED;

    /** Count of threads which are used for warm up dump files reading. */
    private int dumpReadThreads = Math.min(
        OPTIMAL_DUMP_READ_THREADS, Runtime.getRuntime().availableProcessors());

    /** Count of threads which are used for warm up pages loading into memory. */
    private int pageLoadThreads = Math.min(
        OPTIMAL_PAGE_LOAD_THREADS, Runtime.getRuntime().availableProcessors());

    /**
     * If enabled, only index partitions will be tracked and warmed up.
     *
     * @return Prewarming of indexes only flag.
     */
    public boolean isIndexesOnly() {
        return indexesOnly;
    }

    /**
     * Sets prewarming of indexes only flag.
     *
     * @param indexesOnly Prewarming of indexes only flag.
     * @return {@code this} for chaining.
     */
    public PrewarmingConfiguration setIndexesOnly(boolean indexesOnly) {
        this.indexesOnly = indexesOnly;

        return this;
    }

    /**
     * If enabled, starting of page memory for this data region will wait for finishing of prewarming process.
     *
     * @return Wait prewarming on start flag.
     */
    public boolean isWaitPrewarmingOnStart() {
        return waitPrewarmingOnStart;
    }

    /**
     * Sets wait prewarming on start flag.
     *
     * @param waitPrewarmingOnStart Wait prewarming on start flag.
     * @return {@code this} for chaining.
     */
    public PrewarmingConfiguration setWaitPrewarmingOnStart(boolean waitPrewarmingOnStart) {
        this.waitPrewarmingOnStart = waitPrewarmingOnStart;

        return this;
    }

    /**
     * Sets prewarming runtime dump delay.
     * Value {@code -1} (default) means that runtime dumps will be disabled
     * and prewarming implementation will dump ids of loaded pages only on node stop.
     *
     * @return Prewarming runtime dump delay.
     */
    public long getRuntimeDumpDelay() {
        return runtimeDumpDelay;
    }

    /**
     * Sets prewarming runtime dump delay.
     *
     * @param runtimeDumpDelay Prewarming runtime dump delay.
     * @return {@code this} for chaining.
     */
    public PrewarmingConfiguration setRuntimeDumpDelay(long runtimeDumpDelay) {
        this.runtimeDumpDelay = runtimeDumpDelay;

        return this;
    }

    /**
     * Specifies count of threads which are used for warm up dump files reading.
     *
     * @return Count of thread which are used for warm up dump files reading.
     */
    public int getDumpReadThreads() {
        return dumpReadThreads;
    }

    /**
     * Sets count of threads which will be used for warm up dump files reading.
     *
     * @param dumpReadThreads Count of threads which will be used for warm up dump files reading.
     * Must be greater than 0.
     * @return {@code this} for chaining.
     */
    public PrewarmingConfiguration setDumpReadThreads(int dumpReadThreads) {
        assert dumpReadThreads > 0;

        this.dumpReadThreads = dumpReadThreads;

        return this;
    }

    /**
     * Specifies count of threads which are used for warm up pages loading into memory.
     *
     * @return Count of threads which are used for warm up pages loading into memory.
     */
    public int getPageLoadThreads() {
        return pageLoadThreads;
    }

    /**
     * Sets count of threads which will be used for warm up pages loading into memory.
     *
     * @param pageLoadThreads Count of threads which will be used for warm up pages loading into memory.
     * Must be greater than 0.
     * @return {@code this} for chaining.
     */
    public PrewarmingConfiguration setPageLoadThreads(int pageLoadThreads) {
        assert pageLoadThreads > 0;

        this.pageLoadThreads = pageLoadThreads;

        return this;
    }
}
