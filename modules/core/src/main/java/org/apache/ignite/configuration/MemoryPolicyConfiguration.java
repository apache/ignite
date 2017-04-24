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
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;

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
 *                          <property name="size" value="#{100 * 1024 * 1024}"/>
 *                      </bean>
 *
 *                      <bean class="org.apache.ignite.configuration.MemoryPolicyConfiguration">
 *                          <property name="name" value="20MB_Region_Eviction"/>
 *                          <property name="size" value="#{20 * 1024 * 1024}"/>
 *                          <property name="pageEvictionMode" value="RANDOM_2_LRU"/>
 *                      </bean>
 *
 *                      <bean class="org.apache.ignite.configuration.MemoryPolicyConfiguration">
 *                          <property name="name" value="25MB_Region_Swapping"/>
 *                          <property name="size" value="#{25 * 1024 * 1024}"/>
 *                          <property name="swapFilePath" value="memoryPolicyExampleSwap"/>
 *                      </bean>
 *                  </list>
 *              </property>
 *     }
 * </pre>
 */
public final class MemoryPolicyConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Memory policy name. */
    private String name;

    /** Memory policy maximum size. */
    private long size;

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
     * @param name Memory policy name.
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
    public long getSize() {
        return size;
    }

    /**
     * Sets maximum memory region size defined by this memory policy. The total size should not be less than 10 MB
     * due to the internal data structures overhead.
     */
    public MemoryPolicyConfiguration setSize(long size) {
        this.size = size;

        return this;
    }

    /**
     * A path to the memory-mapped file the memory region defined by this memory policy will be mapped to. Having
     * the path set, allows relying on swapping capabilities of an underlying operating system for the memory region.
     *
     * @return A path to the memory-mapped file or {@code null} if this feature is not used for the memory region defined
     *         by this memory policy.
     */
    public String getSwapFilePath() {
        return swapFilePath;
    }

    /**
     * Sets a path to the memory-mapped file.
     *
     * @param swapFilePath A Path to the memory mapped file.
     */
    public MemoryPolicyConfiguration setSwapFilePath(String swapFilePath) {
        this.swapFilePath = swapFilePath;

        return this;
    }

    /**
     * Gets memory pages eviction mode. If {@link DataPageEvictionMode#DISABLED} is used (default) then an out of
     * memory exception will be thrown if the memory region usage, defined by this memory policy, goes beyond its
     * capacity which is {@link #getSize()}.
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
     */
    public MemoryPolicyConfiguration setEmptyPagesPoolSize(int emptyPagesPoolSize) {
        this.emptyPagesPoolSize = emptyPagesPoolSize;

        return this;
    }
}
