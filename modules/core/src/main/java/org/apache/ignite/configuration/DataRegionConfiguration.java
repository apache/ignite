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
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.mxbean.DataRegionMetricsMXBean;
import org.apache.ignite.mxbean.MetricsMxBean;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_DATA_REG_DEFAULT_NAME;

/**
 * This class allows defining custom data regions' configurations with various parameters for Apache Ignite
 * page memory (see {@link DataStorageConfiguration}. For each configured data region Apache Ignite instantiates
 * respective memory regions with different parameters like maximum size, eviction policy, swapping options,
 * persistent mode flag, etc.
 * An Apache Ignite cache can be mapped to a particular region using
 * {@link CacheConfiguration#setDataRegionName(String)} method.
 * <p>Sample configuration below shows how to configure several data regions:</p>
 * <pre>
 *     {@code
 *     <property name="memoryConfiguration">
 *         <bean class="org.apache.ignite.configuration.DataStorageConfiguration">
 *             <property name="defaultRegionConfiguration">
 *                 <bean class="org.apache.ignite.configuration.DataRegionConfiguration">
 *                     <property name="name" value="Default_Region"/>
 *                     <property name="initialSize" value="#{100L * 1024 * 1024}"/>
 *                     <property name="maxSize" value="#{5L * 1024 * 1024 * 1024}"/>
 *                 </bean>
 *             </property>
 *
 *             <property name="pageSize" value="4096"/>
 *
 *             <property name="dataRegions">
 *                 <list>
 *                      <bean class="org.apache.ignite.configuration.DataRegionConfiguration">
 *                          <property name="name" value="20MB_Region_Eviction"/>
 *                          <property name="initialSize" value="#{20L * 1024 * 1024}"/>
 *                          <property name="pageEvictionMode" value="RANDOM_2_LRU"/>
 *                      </bean>
 *
 *                      <bean class="org.apache.ignite.configuration.DataRegionConfiguration">
 *                          <property name="name" value="25MB_Region_Swapping"/>
 *                          <property name="initialSize" value="#{25L * 1024 * 1024}"/>
 *                          <property name="maxSize" value="#{100L * 1024 * 1024}"/>
 *                          <property name="swapPath" value="db/swap"/>
 *                      </bean>
 *                  </list>
 *              </property>
 *     }
 * </pre>
 */
public final class DataRegionConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default metrics enabled flag. */
    public static final boolean DFLT_METRICS_ENABLED = false;

    /** Default amount of sub intervals to calculate {@link DataRegionMetrics#getAllocationRate()} metric. */
    public static final int DFLT_SUB_INTERVALS = 5;

    /** Default length of interval over which {@link DataRegionMetrics#getAllocationRate()} metric is calculated. */
    public static final int DFLT_RATE_TIME_INTERVAL_MILLIS = 60_000;

    /** Data region name. */
    private String name = DFLT_DATA_REG_DEFAULT_NAME;

    /** Data region maximum size in memory. */
    private long maxSize = DataStorageConfiguration.DFLT_DATA_REGION_MAX_SIZE;

    /** Data region start size. */
    private long initSize = Math.min(
        DataStorageConfiguration.DFLT_DATA_REGION_MAX_SIZE, DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE);

    /** An optional path to a memory mapped files directory for this data region. */
    private String swapPath;

    /** An algorithm for memory pages eviction. */
    private DataPageEvictionMode pageEvictionMode = DataPageEvictionMode.DISABLED;

    /**
     * A threshold for memory pages eviction initiation. For instance, if the threshold is 0.9 it means that the page
     * memory will start the eviction only after 90% data region is occupied.
     */
    private double evictionThreshold = 0.9;

    /** Minimum number of empty pages in reuse lists. */
    private int emptyPagesPoolSize = 100;

    /**
     * Flag to enable the memory metrics collection for this data region.
     */
    private boolean metricsEnabled = DFLT_METRICS_ENABLED;

    /** Number of sub-intervals the whole {@link #setMetricsRateTimeInterval(long)} will be split into to calculate
     * {@link DataRegionMetrics#getAllocationRate()} and {@link DataRegionMetrics#getEvictionRate()} rates (5 by default).
     * <p>
     * Setting it to a bigger value will result in more precise calculation and smaller drops of
     * {@link DataRegionMetrics#getAllocationRate()} metric when next sub-interval has to be recycled but introduces bigger
     * calculation overhead. */
    private int metricsSubIntervalCount = DFLT_SUB_INTERVALS;

    /**
     * Time interval (in milliseconds) for {@link DataRegionMetrics#getAllocationRate()}
     * and {@link DataRegionMetrics#getEvictionRate()} monitoring purposes.
     * <p>
     * For instance, after setting the interval to 60_000 milliseconds, subsequent calls to {@link DataRegionMetrics#getAllocationRate()}
     * will return average allocation rate (pages per second) for the last minute.
     */
    private long metricsRateTimeInterval = DFLT_RATE_TIME_INTERVAL_MILLIS;

    /**
     * Flag to enable Ignite Native Persistence.
     */
    private boolean persistenceEnabled = false;

    /** Temporary buffer size for checkpoints in bytes. */
    private long checkpointPageBufSize;

    /**
     * If {@code true}, memory for {@code DataRegion} will be allocated only on the creation of the first cache
     * belonged to this {@code DataRegion}.
     *
     * Default is {@code true}.
     */
    private boolean lazyMemoryAllocation = true;

    /**
     * Gets data region name.
     *
     * @return Data region name.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets data region name. The name must be non empty and must not be equal to the reserved 'sysMemPlc' one.
     *
     * If not specified, {@link DataStorageConfiguration#DFLT_DATA_REG_DEFAULT_NAME} value is used.
     *
     * @param name Data region name.
     * @return {@code this} for chaining.
     */
    public DataRegionConfiguration setName(String name) {
        this.name = name;

        return this;
    }

    /**
     * Maximum memory region size defined by this data region. If the whole data can not fit into the memory region
     * an out of memory exception will be thrown.
     *
     * @return Size in bytes.
     */
    public long getMaxSize() {
        return maxSize;
    }

    /**
     * Sets maximum memory region size defined by this data region. The total size should not be less than 10 MB
     * due to the internal data structures overhead.
     *
     * @param maxSize Maximum data region size in bytes.
     * @return {@code this} for chaining.
     */
    public DataRegionConfiguration setMaxSize(long maxSize) {
        this.maxSize = maxSize;

        return this;
    }

    /**
     * Gets initial memory region size defined by this data region. When the used memory size exceeds this value,
     * new chunks of memory will be allocated.
     *
     * @return Data region start size.
     */
    public long getInitialSize() {
        return initSize;
    }

    /**
     * Sets initial memory region size defined by this data region. When the used memory size exceeds this value,
     * new chunks of memory will be allocated.
     *
     * @param initSize Data region initial size.
     * @return {@code this} for chaining.
     */
    public DataRegionConfiguration setInitialSize(long initSize) {
        this.initSize = initSize;

        return this;
    }

    /**
     * A path to the memory-mapped files the memory region defined by this data region will be mapped to. Having
     * the path set, allows relying on swapping capabilities of an underlying operating system for the memory region.
     *
     * @return A path to the memory-mapped files or {@code null} if this feature is not used for the memory region
     *         defined by this data region.
     */
    public String getSwapPath() {
        return swapPath;
    }

    /**
     * Sets a path to the memory-mapped files.
     *
     * @param swapPath A Path to the memory mapped file.
     * @return {@code this} for chaining.
     */
    public DataRegionConfiguration setSwapPath(String swapPath) {
        this.swapPath = swapPath;

        return this;
    }

    /**
     * Gets memory pages eviction mode. If {@link DataPageEvictionMode#DISABLED} is used (default) then an out of
     * memory exception will be thrown if the memory region usage, defined by this data region, goes beyond its
     * capacity which is {@link #getMaxSize()}.
     *
     * @return Memory pages eviction algorithm. {@link DataPageEvictionMode#DISABLED} used by default.
     */
    public DataPageEvictionMode getPageEvictionMode() {
        return pageEvictionMode;
    }

    /**
     * Sets memory pages eviction mode.
     *
     * @param evictionMode Eviction mode.
     * @return {@code this} for chaining.
     */
    public DataRegionConfiguration setPageEvictionMode(DataPageEvictionMode evictionMode) {
        pageEvictionMode = evictionMode;

        return this;
    }

    /**
     * Gets a threshold for memory pages eviction initiation. For instance, if the threshold is 0.9 it means that the
     * page memory will start the eviction only after 90% of the data region is occupied.
     *
     * @return Memory pages eviction threshold.
     */
    public double getEvictionThreshold() {
        return evictionThreshold;
    }

    /**
     * Sets memory pages eviction threshold.
     *
     * @param evictionThreshold Eviction threshold.
     * @return {@code this} for chaining.
     */
    public DataRegionConfiguration setEvictionThreshold(double evictionThreshold) {
        this.evictionThreshold = evictionThreshold;

        return this;
    }

    /**
     * Specifies the minimal number of empty pages to be present in reuse lists for this data region.
     * This parameter ensures that Ignite will be able to successfully evict old data entries when the size of
     * (key, value) pair is slightly larger than page size / 2.
     * Increase this parameter if cache can contain very big entries (total size of pages in this pool should be enough
     * to contain largest cache entry).
     * Increase this parameter if {@link IgniteOutOfMemoryException} occurred with enabled page eviction.
     *
     * @return Minimum number of empty pages in reuse list.
     */
    public int getEmptyPagesPoolSize() {
        return emptyPagesPoolSize;
    }

    /**
     * Specifies the minimal number of empty pages to be present in reuse lists for this data region.
     * This parameter ensures that Ignite will be able to successfully evict old data entries when the size of
     * (key, value) pair is slightly larger than page size / 2.
     * Increase this parameter if cache can contain very big entries (total size of pages in this pool should be enough
     * to contain largest cache entry).
     * Increase this parameter if {@link IgniteOutOfMemoryException} occurred with enabled page eviction.
     *
     * @param emptyPagesPoolSize Empty pages pool size.
     * @return {@code this} for chaining.
     */
    public DataRegionConfiguration setEmptyPagesPoolSize(int emptyPagesPoolSize) {
        this.emptyPagesPoolSize = emptyPagesPoolSize;

        return this;
    }

    /**
     * Gets whether memory metrics are enabled by default on node startup. Memory metrics can be enabled and disabled
     * at runtime via memory metrics {@link DataRegionMetricsMXBean MX bean}.
     *
     * @return Metrics enabled flag.
     */
    public boolean isMetricsEnabled() {
        return metricsEnabled;
    }

    /**
     * Sets memory metrics enabled flag. If this flag is {@code true}, metrics will be enabled on node startup.
     * Memory metrics can be enabled and disabled at runtime via memory metrics {@link DataRegionMetricsMXBean MX bean}.
     *
     * @param metricsEnabled Metrics enabled flag.
     * @return {@code this} for chaining.
     */
    public DataRegionConfiguration setMetricsEnabled(boolean metricsEnabled) {
        this.metricsEnabled = metricsEnabled;

        return this;
    }

    /**
     * Gets whether persistence is enabled for this data region. All caches residing in this region will be persistent.
     *
     * @return Persistence enabled flag.
     */
    public boolean isPersistenceEnabled() {
        return persistenceEnabled;
    }

    /**
     * Sets persistence enabled flag.
     *
     * @param persistenceEnabled Persistence enabled flag.
     * @return {@code this} for chaining.
     */
    public DataRegionConfiguration setPersistenceEnabled(boolean persistenceEnabled) {
        this.persistenceEnabled = persistenceEnabled;

        return this;
    }

    /**
     * Gets time interval for {@link DataRegionMetrics#getAllocationRate()}
     * and {@link DataRegionMetrics#getEvictionRate()} monitoring purposes.
     * <p>
     * For instance, after setting the interval to 60_000 milliseconds,
     * subsequent calls to {@link DataRegionMetrics#getAllocationRate()}
     * will return average allocation rate (pages per second) for the last minute.
     *
     * @return Time interval over which allocation rate is calculated.
     * @deprecated Use {@link MetricsMxBean#configureHitRateMetric(String, long)} instead.
     */
    @Deprecated
    public long getMetricsRateTimeInterval() {
        return metricsRateTimeInterval;
    }

    /**
     * Sets time interval for {@link DataRegionMetrics#getAllocationRate()}
     * and {@link DataRegionMetrics#getEvictionRate()} monitoring purposes.
     * <p>
     * For instance, after setting the interval to 60 seconds,
     * subsequent calls to {@link DataRegionMetrics#getAllocationRate()}
     * will return average allocation rate (pages per second) for the last minute.
     *
     * @param metricsRateTimeInterval Time interval used for allocation and eviction rates calculations.
     * @return {@code this} for chaining.
     * @deprecated Use {@link MetricsMxBean#configureHitRateMetric(String, long)} instead.
     */
    @Deprecated
    public DataRegionConfiguration setMetricsRateTimeInterval(long metricsRateTimeInterval) {
        this.metricsRateTimeInterval = metricsRateTimeInterval;

        return this;
    }

    /**
     * Gets a number of sub-intervals the whole {@link #setMetricsRateTimeInterval(long)}
     * will be split into to calculate {@link DataRegionMetrics#getAllocationRate()}
     * and {@link DataRegionMetrics#getEvictionRate()} rates (5 by default).
     * <p>
     * Setting it to a bigger value will result in more precise calculation and smaller drops of
     * {@link DataRegionMetrics#getAllocationRate()} metric when next sub-interval has to be recycled but introduces bigger
     * calculation overhead.
     *
     * @return number of sub intervals.
     * @deprecated Use {@link MetricsMxBean#configureHitRateMetric(String, long)} instead.
     */
    @Deprecated
    public int getMetricsSubIntervalCount() {
        return metricsSubIntervalCount;
    }

    /**
     * Sets a number of sub-intervals the whole {@link #setMetricsRateTimeInterval(long)} will be split into to calculate
     * {@link DataRegionMetrics#getAllocationRate()} and {@link DataRegionMetrics#getEvictionRate()} rates (5 by default).
     * <p>
     * Setting it to a bigger value will result in more precise calculation and smaller drops of
     * {@link DataRegionMetrics#getAllocationRate()} metric when next sub-interval has to be recycled but introduces bigger
     * calculation overhead.
     *
     * @param metricsSubIntervalCnt A number of sub-intervals.
     * @return {@code this} for chaining.
     * @deprecated Use {@link MetricsMxBean#configureHitRateMetric(String, long)} instead.
     */
    @Deprecated
    public DataRegionConfiguration setMetricsSubIntervalCount(int metricsSubIntervalCnt) {
        this.metricsSubIntervalCount = metricsSubIntervalCnt;

        return this;
    }

    /**
     * Gets amount of memory allocated for a checkpoint temporary buffer.
     *
     * @return Checkpoint page buffer size in bytes or {@code 0} for Ignite
     *      to choose the buffer size automatically.
     */
    public long getCheckpointPageBufferSize() {
        return checkpointPageBufSize;
    }

    /**
     * Sets amount of memory allocated for the checkpoint temporary buffer. The buffer is used to create temporary
     * copies of pages that are being written to disk and being update in parallel while the checkpoint is in
     * progress.
     *
     * @param checkpointPageBufSize Checkpoint page buffer size in bytes or {@code 0} for Ignite to
     *      choose the buffer size automatically.
     * @return {@code this} for chaining.
     */
    public DataRegionConfiguration setCheckpointPageBufferSize(long checkpointPageBufSize) {
        this.checkpointPageBufSize = checkpointPageBufSize;

        return this;
    }

    /**
     * @return {@code True} if memory for {@code DataRegion} will be allocated only on the creation of the first cache
     * belonged to this {@code DataRegion}.
     */
    public boolean isLazyMemoryAllocation() {
        return lazyMemoryAllocation;
    }

    /**
     * Sets {@code lazyMemoryAllocation} flag value.
     *
     * If {@code true}, memory for {@code DataRegion} will be allocated only on the creation of the first cache
     * belonged to this {@code DataRegion}.
     *
     * @param lazyMemoryAllocation Flag value.
     * @return {@code this} for chaining.
     */
    public DataRegionConfiguration setLazyMemoryAllocation(boolean lazyMemoryAllocation) {
        this.lazyMemoryAllocation = lazyMemoryAllocation;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DataRegionConfiguration.class, this);
    }
}
