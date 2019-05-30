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
import java.util.Map;
import java.util.function.Supplier;

/**
 * This class defines page memory prewarming configuration.
 */
public class PrewarmingConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Runtime dump disabled. */
    public static final long RUNTIME_DUMP_DISABLED = -1;

    /** Default hottest zone ratio. */
    public static final double DFLT_HOTTEST_ZONE_RATIO = 0.25;

    /**
     * Optimal count of threads for warm up pages loading into memory.
     * That value was obtained through testing warm up functionality on 28 cores with hyperThreading.
     */
    public static final int OPTIMAL_PAGE_LOAD_THREADS = 16;

    /** Default throttle accuracy. */
    public static final double DFLT_THROTTLE_ACCURACY = 0.25;

    /** Supplier of partition page indexes which assumed as supplier of all pages in the partition. */
    public static final Supplier<int[]> WHOLE_PARTITION = () -> null;

    /** Prewarming of indexes only flag. */
    private boolean indexesOnly;

    /** Wait prewarming on start flag. */
    private boolean waitPrewarmingOnStart;

    /** Prewarming runtime dump delay. */
    private long runtimeDumpDelay = RUNTIME_DUMP_DISABLED;

    /** Hottest zone ratio. */
    private double hottestZoneRatio = DFLT_HOTTEST_ZONE_RATIO;

    /** Count of threads which are used for loading pages into memory. */
    private int pageLoadThreads = Math.min(
        OPTIMAL_PAGE_LOAD_THREADS, Runtime.getRuntime().availableProcessors());

    /** Prewarming throttle accuracy. */
    private double throttleAccuracy = DFLT_THROTTLE_ACCURACY;

    /** Custom supplier of page IDs to prewarm. */
    private Supplier<Map<String, Map<Integer, Supplier<int[]>>>> customPageIdsSupplier;

    /**
     * If enabled, only index partitions will be prewarmed.
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
     * @return Hottest zone ratio.
     */
    public double getHottestZoneRatio() {
        return hottestZoneRatio;
    }

    /**
     * @param hottestZoneRatio New hottest zone ratio.
     * @return {@code this} for chaining.
     */
    public PrewarmingConfiguration setHottestZoneRatio(double hottestZoneRatio) {
        this.hottestZoneRatio = hottestZoneRatio;

        return this;
    }

    /**
     * Specifies count of threads which are used for loading pages into memory.
     *
     * @return Count of threads which are used for loading pages into memory.
     */
    public int getPageLoadThreads() {
        return pageLoadThreads;
    }

    /**
     * Sets count of threads which will be used for loading pages into memory.
     *
     * @param pageLoadThreads Count of threads which will be used for loading pages into memory.
     * Must be greater than 0.
     * @return {@code this} for chaining.
     */
    public PrewarmingConfiguration setPageLoadThreads(int pageLoadThreads) {
        assert pageLoadThreads > 0;

        this.pageLoadThreads = pageLoadThreads;

        return this;
    }

    /**
     * @return Prewarming throttle accuracy.
     */
    public double getThrottleAccuracy() {
        return throttleAccuracy;
    }

    /**
     * @param throttleAccuracy New prewarming throttle accuracy.
     */
    public PrewarmingConfiguration setThrottleAccuracy(double throttleAccuracy) {
        this.throttleAccuracy = throttleAccuracy;

        return this;
    }

    /**
     * Gets custom supplier of page IDs to prewarm.
     * It should supply a map, which keys will assumed as cache names, and values as maps of partition IDs in that cache
     * to suppliers of page indexes arrays in that partition. Pages will be loaded in order which provided by supplier.
     *
     * @see #setCustomPageIdsSupplier(Supplier)
     * @return Custom supplier of page IDs to prewarm.
     */
    public Supplier<Map<String, Map<Integer, Supplier<int[]>>>> getCustomPageIdsSupplier() {
        return customPageIdsSupplier;
    }

    /**
     * Sets custom supplier of page IDs to prewarm.
     * If not set, default supplier which provides IDs of loaded pages before last shutdown will be used.
     *
     * @see #getCustomPageIdsSupplier()
     * @param customPageIdsSupplier New custom supplier of page IDs to prewarm.
     */
    public void setCustomPageIdsSupplier(Supplier<Map<String, Map<Integer, Supplier<int[]>>>> customPageIdsSupplier) {
        this.customPageIdsSupplier = customPageIdsSupplier;
    }
}
