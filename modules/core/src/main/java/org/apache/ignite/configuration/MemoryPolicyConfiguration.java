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
import org.apache.ignite.MemoryMetrics;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.mxbean.DataRegionMetricsMXBean;

import static org.apache.ignite.configuration.MemoryConfiguration.DFLT_MEM_PLC_DEFAULT_NAME;

/**
 * This class allows defining custom memory policies' configurations with various parameters for Apache Ignite
 * page memory (see {@link MemoryConfiguration}. For each configured memory policy Apache Ignite instantiates
 * respective memory regions with different parameters like maximum size, eviction policy, swapping options, etc.
 * An Apache Ignite cache can be mapped to a particular policy using
 * {@link CacheConfiguration#setMemoryPolicyName(String)} method.
 * <p>Sample configuration below shows how to configure several memory policies:</p>
 * <pre>
 *     {@code
 *     <property name="memoryConfiguration">
 *         <bean class="org.apache.ignite.configuration.MemoryConfiguration">
 *             <property name="defaultMemoryPolicyName" value="Default_Region"/>
 *             <property name="pageSize" value="4096"/>
 *
 *             <property name="memoryPolicies">
 *                 <list>
 *                      <bean class="org.apache.ignite.configuration.MemoryPolicyConfiguration">
 *                          <property name="name" value="Default_Region"/>
 *                          <property name="initialSize" value="#{100L * 1024 * 1024}"/>
 *                      </bean>
 *
 *                      <bean class="org.apache.ignite.configuration.MemoryPolicyConfiguration">
 *                          <property name="name" value="20MB_Region_Eviction"/>
 *                          <property name="initialSize" value="#{20L * 1024 * 1024}"/>
 *                          <property name="pageEvictionMode" value="RANDOM_2_LRU"/>
 *                      </bean>
 *
 *                      <bean class="org.apache.ignite.configuration.MemoryPolicyConfiguration">
 *                          <property name="name" value="25MB_Region_Swapping"/>
 *                          <property name="initialSize" value="#{25L * 1024 * 1024}"/>
 *                          <property name="maxSize" value="#{100L * 1024 * 1024}"/>
 *                          <property name="swapFilePath" value="memoryPolicyExampleSwap"/>
 *                      </bean>
 *                  </list>
 *              </property>
 *     }
 * </pre>
 *
 * @deprecated Use {@link DataRegionConfiguration} instead.
 */
@Deprecated
public final class MemoryPolicyConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default metrics enabled flag. */
    public static final boolean DFLT_METRICS_ENABLED = false;

    /** Default amount of sub intervals to calculate {@link MemoryMetrics#getAllocationRate()} metric. */
    public static final int DFLT_SUB_INTERVALS = 5;

    /** Default length of interval over which {@link MemoryMetrics#getAllocationRate()} metric is calculated. */
    public static final int DFLT_RATE_TIME_INTERVAL_MILLIS = 60_000;

    /** Memory policy name. */
    private String name = DFLT_MEM_PLC_DEFAULT_NAME;

    /** Memory policy start size. */
    private long initialSize;

    /** Memory policy maximum size. */
    private long maxSize = MemoryConfiguration.DFLT_MEMORY_POLICY_MAX_SIZE;

    /** An optional path to a memory mapped file for this memory policy. */
    private String swapFilePath;

    /** An algorithm for memory pages eviction. */
    private DataPageEvictionMode pageEvictionMode = DataPageEvictionMode.DISABLED;

    /**
     * A threshold for memory pages eviction initiation. For instance, if the threshold is 0.9 it means that the page
     * memory will start the eviction only after 90% memory region (defined by this policy) is occupied.
     */
    private double evictionThreshold = 0.9;

    /** Minimum number of empty pages in reuse lists. */
    private int emptyPagesPoolSize = 100;

    /**
     * Flag to enable the memory metrics collection for this memory policy.
     */
    private boolean metricsEnabled = DFLT_METRICS_ENABLED;

    /** Number of sub-intervals the whole {@link #setRateTimeInterval(long)} will be split into to calculate
     * {@link MemoryMetrics#getAllocationRate()} and {@link MemoryMetrics#getEvictionRate()} rates (5 by default).
     * <p>
     * Setting it to a bigger value will result in more precise calculation and smaller drops of
     * {@link MemoryMetrics#getAllocationRate()} metric when next sub-interval has to be recycled but introduces bigger
     * calculation overhead. */
    private int subIntervals = DFLT_SUB_INTERVALS;

    /**
     * Time interval (in milliseconds) for {@link MemoryMetrics#getAllocationRate()}
     * and {@link MemoryMetrics#getEvictionRate()} monitoring purposes.
     * <p>
     * For instance, after setting the interval to 60_000 milliseconds, subsequent calls to {@link MemoryMetrics#getAllocationRate()}
     * will return average allocation rate (pages per second) for the last minute.
     */
    private long rateTimeInterval = DFLT_RATE_TIME_INTERVAL_MILLIS;

    /**
     * Gets memory policy name.
     *
     * @return Memory policy name.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets memory policy name. The name must be non empty and must not be equal to the reserved 'sysMemPlc' one.
     *
     * If not specified, {@link MemoryConfiguration#DFLT_MEM_PLC_DEFAULT_NAME} value is used.
     *
     * @param name Memory policy name.
     * @return {@code this} for chaining.
     */
    public MemoryPolicyConfiguration setName(String name) {
        this.name = name;

        return this;
    }

    /**
     * Maximum memory region size defined by this memory policy. If the whole data can not fit into the memory region
     * an out of memory exception will be thrown.
     *
     * @return Size in bytes.
     */
    public long getMaxSize() {
        return maxSize;
    }

    /**
     * Sets maximum memory region size defined by this memory policy. The total size should not be less than 10 MB
     * due to the internal data structures overhead.
     *
     * @param maxSize Maximum memory policy size in bytes.
     * @return {@code this} for chaining.
     */
    public MemoryPolicyConfiguration setMaxSize(long maxSize) {
        this.maxSize = maxSize;

        return this;
    }

    /**
     * Gets initial memory region size defined by this memory policy. When the used memory size exceeds this value,
     * new chunks of memory will be allocated.
     *
     * @return Memory policy start size.
     */
    public long getInitialSize() {
        return initialSize;
    }

    /**
     * Sets initial memory region size defined by this memory policy. When the used memory size exceeds this value,
     * new chunks of memory will be allocated.
     *
     * @param initialSize Memory policy initial size.
     * @return {@code this} for chaining.
     */
    public MemoryPolicyConfiguration setInitialSize(long initialSize) {
        this.initialSize = initialSize;

        return this;
    }

    /**
     * A path to the memory-mapped files the memory region defined by this memory policy will be mapped to. Having
     * the path set, allows relying on swapping capabilities of an underlying operating system for the memory region.
     *
     * @return A path to the memory-mapped files or {@code null} if this feature is not used for the memory region
     *         defined by this memory policy.
     */
    public String getSwapFilePath() {
        return swapFilePath;
    }

    /**
     * Sets a path to the memory-mapped file.
     *
     * @param swapFilePath A Path to the memory mapped file.
     * @return {@code this} for chaining.
     */
    public MemoryPolicyConfiguration setSwapFilePath(String swapFilePath) {
        this.swapFilePath = swapFilePath;

        return this;
    }

    /**
     * Gets memory pages eviction mode. If {@link DataPageEvictionMode#DISABLED} is used (default) then an out of
     * memory exception will be thrown if the memory region usage, defined by this memory policy, goes beyond its
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
    public MemoryPolicyConfiguration setPageEvictionMode(DataPageEvictionMode evictionMode) {
        pageEvictionMode = evictionMode;

        return this;
    }

    /**
     * Gets a threshold for memory pages eviction initiation. For instance, if the threshold is 0.9 it means that the
     * page memory will start the eviction only after 90% of the memory region (defined by this policy) is occupied.
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
    public MemoryPolicyConfiguration setEvictionThreshold(double evictionThreshold) {
        this.evictionThreshold = evictionThreshold;

        return this;
    }

    /**
     * Specifies the minimal number of empty pages to be present in reuse lists for this memory policy.
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
     * Specifies the minimal number of empty pages to be present in reuse lists for this memory policy.
     * This parameter ensures that Ignite will be able to successfully evict old data entries when the size of
     * (key, value) pair is slightly larger than page size / 2.
     * Increase this parameter if cache can contain very big entries (total size of pages in this pool should be enough
     * to contain largest cache entry).
     * Increase this parameter if {@link IgniteOutOfMemoryException} occurred with enabled page eviction.
     *
     * @param emptyPagesPoolSize Empty pages pool size.
     * @return {@code this} for chaining.
     */
    public MemoryPolicyConfiguration setEmptyPagesPoolSize(int emptyPagesPoolSize) {
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
    public MemoryPolicyConfiguration setMetricsEnabled(boolean metricsEnabled) {
        this.metricsEnabled = metricsEnabled;

        return this;
    }

    /**
     * Gets time interval for {@link MemoryMetrics#getAllocationRate()}
     * and {@link MemoryMetrics#getEvictionRate()} monitoring purposes.
     * <p>
     * For instance, after setting the interval to 60_000 milliseconds,
     * subsequent calls to {@link MemoryMetrics#getAllocationRate()}
     * will return average allocation rate (pages per second) for the last minute.
     *
     * @return Time interval over which allocation rate is calculated.
     */
    public long getRateTimeInterval() {
        return rateTimeInterval;
    }

    /**
     * Sets time interval for {@link MemoryMetrics#getAllocationRate()}
     * and {@link MemoryMetrics#getEvictionRate()} monitoring purposes.
     * <p>
     * For instance, after setting the interval to 60 seconds,
     * subsequent calls to {@link MemoryMetrics#getAllocationRate()}
     * will return average allocation rate (pages per second) for the last minute.
     *
     * @param rateTimeInterval Time interval used for allocation and eviction rates calculations.
     * @return {@code this} for chaining.
     */
    public MemoryPolicyConfiguration setRateTimeInterval(long rateTimeInterval) {
        this.rateTimeInterval = rateTimeInterval;

        return this;
    }

    /**
     * Gets a number of sub-intervals the whole {@link #setRateTimeInterval(long)}
     * will be split into to calculate {@link MemoryMetrics#getAllocationRate()}
     * and {@link MemoryMetrics#getEvictionRate()} rates (5 by default).
     * <p>
     * Setting it to a bigger value will result in more precise calculation and smaller drops of
     * {@link MemoryMetrics#getAllocationRate()} metric when next sub-interval has to be recycled but introduces bigger
     * calculation overhead.
     *
     * @return number of sub intervals.
     */
    public int getSubIntervals() {
        return subIntervals;
    }

    /**
     * Sets a number of sub-intervals the whole {@link #setRateTimeInterval(long)} will be split into to calculate
     * {@link MemoryMetrics#getAllocationRate()} and {@link MemoryMetrics#getEvictionRate()} rates (5 by default).
     * <p>
     * Setting it to a bigger value will result in more precise calculation and smaller drops of
     * {@link MemoryMetrics#getAllocationRate()} metric when next sub-interval has to be recycled but introduces bigger
     * calculation overhead.
     *
     * @param subIntervals A number of sub-intervals.
     * @return {@code this} for chaining.
     */
    public MemoryPolicyConfiguration setSubIntervals(int subIntervals) {
        this.subIntervals = subIntervals;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MemoryPolicyConfiguration.class, this);
    }
}
