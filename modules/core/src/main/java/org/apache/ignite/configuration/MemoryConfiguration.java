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
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * A page memory configuration for an Apache Ignite node. The page memory is a manageable off-heap based memory
 * architecture that divides all expandable memory regions into pages of fixed size
 * (see {@link MemoryConfiguration#getPageSize()}. An individual page can store one or many cache key-value entries
 * that allows reusing the memory in the most efficient way and avoid memory fragmentation issues.
 * <p>
 * By default, the page memory allocates a single expandable memory region using settings of
 * {@link MemoryConfiguration#createDefaultPolicyConfig()}. All the caches that will be configured in an application
 * will be mapped to this memory region by default, thus, all the cache data will reside in that memory region.
 * <p>
 * If initial size of the default memory region doesn't satisfy requirements or it's required to have multiple memory
 * regions with different properties then {@link MemoryPolicyConfiguration} can be used for both scenarios.
 * For instance, using memory policies you can define memory regions of different maximum size, eviction policies,
 * swapping options, etc. Once you define a new memory region you can bind particular Ignite caches to it.
 * <p>
 * To learn more about memory policies refer to {@link MemoryPolicyConfiguration} documentation.
 * <p>Sample configuration below shows how to make 5 GB memory regions the default one for Apache Ignite:</p>
 * <pre>
 *     {@code
 *     <property name="memoryConfiguration">
 *         <bean class="org.apache.ignite.configuration.MemoryConfiguration">
 *             <property name="systemCacheInitialSize" value="#{100L * 1024 * 1024}"/>
 *             <property name="defaultMemoryPolicyName" value="default_mem_plc"/>
 *
 *             <property name="memoryPolicies">
 *                 <list>
 *                     <bean class="org.apache.ignite.configuration.MemoryPolicyConfiguration">
 *                         <property name="name" value="default_mem_plc"/>
 *                         <property name="initialSize" value="#{5L * 1024 * 1024 * 1024}"/>
 *                     </bean>
 *                 </list>
 *             </property>
 *         </bean>
 *     </property>
 *     }
 * </pre>
 *
 * @deprecated Use {@link DataStorageConfiguration} instead.
 */
@Deprecated
public class MemoryConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default memory policy start size (256 MB). */
    @SuppressWarnings("UnnecessaryBoxing")
    public static final long DFLT_MEMORY_POLICY_INITIAL_SIZE = 256L * 1024 * 1024;

    /** Fraction of available memory to allocate for default DataRegion. */
    private static final double DFLT_MEMORY_POLICY_FRACTION = 0.2;

    /** Default memory policy's size is 20% of physical memory available on current machine. */
    public static final long DFLT_MEMORY_POLICY_MAX_SIZE = Math.max(
        (long)(DFLT_MEMORY_POLICY_FRACTION * U.getTotalMemoryAvailable()),
        DFLT_MEMORY_POLICY_INITIAL_SIZE);

    /** Default initial size of a memory chunk for the system cache (40 MB). */
    private static final long DFLT_SYS_CACHE_INIT_SIZE = 40L * 1024 * 1024;

    /** Default max size of a memory chunk for the system cache (100 MB). */
    private static final long DFLT_SYS_CACHE_MAX_SIZE = 100L * 1024 * 1024;

    /** Default memory page size. */
    public static final int DFLT_PAGE_SIZE = 4 * 1024;

    /** This name is assigned to default DataRegion if no user-defined default MemPlc is specified */
    public static final String DFLT_MEM_PLC_DEFAULT_NAME = "default";

    /** Size of a memory chunk reserved for system cache initially. */
    private long sysCacheInitSize = DFLT_SYS_CACHE_INIT_SIZE;

    /** Maximum size of system cache. */
    private long sysCacheMaxSize = DFLT_SYS_CACHE_MAX_SIZE;

    /** Memory page size. */
    private int pageSize;

    /** Concurrency level. */
    private int concLvl;

    /** A name of the memory policy that defines the default memory region. */
    private String dfltMemPlcName = DFLT_MEM_PLC_DEFAULT_NAME;

    /** Size of memory (in bytes) to use for default DataRegion. */
    private long dfltMemPlcSize = DFLT_MEMORY_POLICY_MAX_SIZE;

    /** Memory policies. */
    private MemoryPolicyConfiguration[] memPlcs;

    /**
     * Initial size of a memory region reserved for system cache.
     *
     * @return Size in bytes.
     */
    public long getSystemCacheInitialSize() {
        return sysCacheInitSize;
    }

    /**
     * Sets initial size of a memory region reserved for system cache.
     *
     * Default value is {@link #DFLT_SYS_CACHE_INIT_SIZE}
     *
     * @param sysCacheInitSize Size in bytes.
     *
     * @return {@code this} for chaining.
     */
    public MemoryConfiguration setSystemCacheInitialSize(long sysCacheInitSize) {
        A.ensure(sysCacheInitSize > 0, "System cache initial size can not be less zero.");

        this.sysCacheInitSize = sysCacheInitSize;

        return this;
    }

    /**
     * Maximum memory region size reserved for system cache.
     *
     * @return Size in bytes.
     */
    public long getSystemCacheMaxSize() {
        return sysCacheMaxSize;
    }

    /**
     * Sets maximum memory region size reserved for system cache. The total size should not be less than 10 MB
     * due to internal data structures overhead.
     *
     * @param sysCacheMaxSize Maximum size in bytes for system cache memory region.
     *
     * @return {@code this} for chaining.
     */
    public MemoryConfiguration setSystemCacheMaxSize(long sysCacheMaxSize) {
        A.ensure(sysCacheMaxSize > 0, "System cache max size can not be less zero.");

        this.sysCacheMaxSize = sysCacheMaxSize;

        return this;
    }

    /**
     * The pages memory consists of one or more expandable memory regions defined by {@link MemoryPolicyConfiguration}.
     * Every memory region is split on pages of fixed size that store actual cache entries.
     *
     * @return Page size in bytes.
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * Changes the page size.
     *
     * Default value is {@link #DFLT_PAGE_SIZE}
     *
     * @param pageSize Page size in bytes.
     */
    public MemoryConfiguration setPageSize(int pageSize) {
        A.ensure(pageSize >= 1024 && pageSize <= 16 * 1024, "Page size must be between 1kB and 16kB.");
        A.ensure(U.isPow2(pageSize), "Page size must be a power of 2.");

        this.pageSize = pageSize;

        return this;
    }

    /**
     * Gets an array of all memory policies configured. Apache Ignite will instantiate a dedicated memory region per
     * policy. An Apache Ignite cache can be mapped to a specific policy with
     * {@link CacheConfiguration#setMemoryPolicyName(String)} method.
     *
     * @return Array of configured memory policies.
     */
    public MemoryPolicyConfiguration[] getMemoryPolicies() {
        return memPlcs;
    }

    /**
     * Sets memory policies configurations.
     *
     * @param memPlcs Memory policies configurations.
     */
    public MemoryConfiguration setMemoryPolicies(MemoryPolicyConfiguration... memPlcs) {
        this.memPlcs = memPlcs;

        return this;
    }

    /**
     * Creates a configuration for the default memory policy that will instantiate the default memory region.
     * To override settings of the default memory policy in order to create the default memory region with different
     * parameters, create own memory policy first, pass it to
     * {@link MemoryConfiguration#setMemoryPolicies(MemoryPolicyConfiguration...)} method and change the name of the
     * default memory policy with {@link MemoryConfiguration#setDefaultMemoryPolicyName(String)}.
     *
     * @return default Memory policy configuration.
     */
    public MemoryPolicyConfiguration createDefaultPolicyConfig() {
        MemoryPolicyConfiguration memPlc = new MemoryPolicyConfiguration();

        long maxSize = dfltMemPlcSize;

        if (maxSize < DFLT_MEMORY_POLICY_INITIAL_SIZE)
            memPlc.setInitialSize(maxSize);
        else
            memPlc.setInitialSize(DFLT_MEMORY_POLICY_INITIAL_SIZE);

        memPlc.setMaxSize(maxSize);

        return memPlc;
    }

    /**
     * Returns the number of concurrent segments in Ignite internal page mapping tables. By default equals
     * to the number of available CPUs multiplied by 4.
     *
     * @return Mapping table concurrency level.
     */
    public int getConcurrencyLevel() {
        return concLvl;
    }

    /**
     * Sets the number of concurrent segments in Ignite internal page mapping tables.
     *
     * @param concLvl Mapping table concurrency level.
     */
    public MemoryConfiguration setConcurrencyLevel(int concLvl) {
        this.concLvl = concLvl;

        return this;
    }

    /**
     * Gets a size for default memory policy overridden by user.
     *
     * @return Default memory policy size overridden by user or {@link #DFLT_MEMORY_POLICY_MAX_SIZE} if nothing was specified.
     */
    public long getDefaultMemoryPolicySize() {
        return dfltMemPlcSize;
    }

    /**
     * Overrides size of default memory policy which is created automatically.
     *
     * If user doesn't specify any memory policy configuration, a default one with default size
     * (20% of available RAM) is created by Ignite.
     *
     * This property allows user to specify desired size of default memory policy
     * without having to use more verbose syntax of MemoryPolicyConfiguration elements.
     *
     * @param dfltMemPlcSize Size of default memory policy overridden by user.
     */
    public MemoryConfiguration setDefaultMemoryPolicySize(long dfltMemPlcSize) {
        this.dfltMemPlcSize = dfltMemPlcSize;

        return this;
    }

    /**
     * Gets a name of default memory policy.
     *
     * @return A name of a custom memory policy configured with {@code MemoryConfiguration} or {@code null} of the
     *         default policy is used.
     */
    public String getDefaultMemoryPolicyName() {
        return dfltMemPlcName;
    }

    /**
     * Sets the name for the default memory policy that will initialize the default memory region.
     * To set own default memory policy, create the policy first, pass it to
     * {@link MemoryConfiguration#setMemoryPolicies(MemoryPolicyConfiguration...)} method and change the name of the
     * default memory policy with {@code MemoryConfiguration#setDefaultMemoryPolicyName(String)}.
     *
     * If nothing is specified by user, it is set to {@link #DFLT_MEM_PLC_DEFAULT_NAME} value.
     *
     * @param dfltMemPlcName Name of a memory policy to be used as default one.
     */
    public MemoryConfiguration setDefaultMemoryPolicyName(String dfltMemPlcName) {
        this.dfltMemPlcName = dfltMemPlcName;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(MemoryConfiguration.class, this);
    }
}
