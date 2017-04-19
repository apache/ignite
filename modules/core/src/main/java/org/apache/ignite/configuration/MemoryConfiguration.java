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
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * A page memory configuration for an Apache Ignite node. The page memory is a manageable off-heap based memory
 * architecture that divides all continuously allocated memory regions into pages of fixed size
 * (see {@link MemoryConfiguration#getPageSize()}. An individual page can store one or many cache key-value entries
 * that allows reusing the memory in the most efficient way and avoid memory fragmentation issues.
 * <p>
 * By default, the page memory allocates a single continuous memory region using settings of
 * {@link MemoryConfiguration#createDefaultPolicyConfig()}. All the caches that will be configured in an application
 * will be mapped to this memory region by default, thus, all the cache data will reside in that memory region.
 * <p>
 * If initial size of the default memory region doesn't satisfy requirements or it's required to have multiple memory
 * regions with different properties then {@link MemoryPolicyConfiguration} can be used for both scenarios.
 * For instance, Using memory policies you can define memory regions of different maximum size, eviction policies,
 * swapping options, etc. Once you define a new memory region you can bind particular Ignite caches to it.
 * <p>
 * To learn more about memory policies refer to {@link MemoryPolicyConfiguration} documentation.
 * <p>Sample configuration below shows how to make 5 GB memory regions the default one for Apache Ignite:</p>
 * <pre>
 *     {@code
 *     <property name="memoryConfiguration">
 *         <bean class="org.apache.ignite.configuration.MemoryConfiguration">
 *             <property name="systemCacheMemorySize" value="#{100 * 1024 * 1024}"/>
 *             <property name="defaultMemoryPolicyName" value="default_mem_plc"/>
 *
 *             <property name="memoryPolicies">
 *                 <list>
 *                     <bean class="org.apache.ignite.configuration.MemoryPolicyConfiguration">
 *                         <property name="name" value="default_mem_plc"/>
 *                         <property name="size" value="#{5 * 1024 * 1024 * 1024}"/>
 *                     </bean>
 *                 </list>
 *             </property>
 *         </bean>
 *     </property>
 *     }
 * </pre>
 */
public class MemoryConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default memory policy's size (1 GB). */
    public static final long DFLT_MEMORY_POLICY_SIZE = 1024 * 1024 * 1024;

    /** Default size of a memory chunk for the system cache (100 MB). */
    public static final long DFLT_SYS_CACHE_MEM_SIZE = 100 * 1024 * 1024;

    /** Default memory page size. */
    public static final int DFLT_PAGE_SIZE = 2 * 1024;

    /** Size of a memory chunk reserved for system cache needs. */
    private long sysCacheMemSize = DFLT_SYS_CACHE_MEM_SIZE;

    /** Memory page size. */
    private int pageSize = DFLT_PAGE_SIZE;

    /** Concurrency level. */
    private int concLvl;

    /** A name of the memory policy that defines the default memory region. */
    private String dfltMemPlcName;

    /** Memory policies. */
    private MemoryPolicyConfiguration[] memPlcs;

    /**
     * Gets size of a memory chunk reserved for system cache needs.
     *
     * @return Size in bytes.
     */
    public long getSystemCacheMemorySize() {
        return sysCacheMemSize;
    }

    /**
     * Sets the size of a memory chunk reserved for system cache needs.
     *
     * @param sysCacheMemSize Size in bytes.
     */
    public MemoryConfiguration setSystemCacheMemorySize(long sysCacheMemSize) {
        this.sysCacheMemSize = sysCacheMemSize;

        return this;
    }

    /**
     * The pages memory consists of one or more continuous memory regions defined by {@link MemoryPolicyConfiguration}.
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
     * Creates a configuration for the default memory policy that will instantiate the default continuous memory region.
     * To override settings of the default memory policy in order to create the default memory region with different
     * parameters, create own memory policy first, pass it to
     * {@link MemoryConfiguration#setMemoryPolicies(MemoryPolicyConfiguration...)} method and change the name of the
     * default memory policy with {@link MemoryConfiguration#setDefaultMemoryPolicyName(String)}.
     *
     * @return default Memory policy configuration.
     */
    public MemoryPolicyConfiguration createDefaultPolicyConfig() {
        MemoryPolicyConfiguration memPlc = new MemoryPolicyConfiguration();

        memPlc.setName(null);
        memPlc.setSize(DFLT_MEMORY_POLICY_SIZE);

        return memPlc;
    }

    /**
     * TODO: document
     * @return Concurrency level.
     */
    public int getConcurrencyLevel() {
        return concLvl;
    }

    /**
     * TODO: document
     * @param concLvl Concurrency level.
     */
    public MemoryConfiguration setConcurrencyLevel(int concLvl) {
        this.concLvl = concLvl;

        return this;
    }

    /**
     * Gets a name of default memory policy.
     *
     * @return A name of a custom memory policy configured with {@link MemoryConfiguration} or {@code null} of the
     *         default policy is used.
     */
    public String getDefaultMemoryPolicyName() {
        return dfltMemPlcName;
    }

    /**
     * Sets the name for the default memory policy that will initialize the default memory region.
     * To set own default memory policy, create the policy first, pass it to
     * {@link MemoryConfiguration#setMemoryPolicies(MemoryPolicyConfiguration...)} method and change the name of the
     * default memory policy with {@link MemoryConfiguration#setDefaultMemoryPolicyName(String)}.
     *
     * @param dfltMemPlcName Name of a memory policy to be used as default one.
     */
    public MemoryConfiguration setDefaultMemoryPolicyName(String dfltMemPlcName) {
        this.dfltMemPlcName = dfltMemPlcName;

        return this;
    }
}
