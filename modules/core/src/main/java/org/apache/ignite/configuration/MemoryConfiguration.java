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
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.database.MemoryPolicy;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Page memory configuration of Apache Ignite node defines memory regions called {@link PageMemory}
 * where all cache data is stored.
 *
 * PageMemory objects and associated data structures are managed by {@link MemoryPolicy} objects.
 *
 * User can define as many memory policies as he or she wants, but there are some validation rules
 * applied to memory configuration provided by user.
 *
 * <p>Validation rules:</p>
 * <ul>
 *     <li>
 *         All user-defined policies must have non-null non-empty unique names.
 *     </li>
 *     <li>
 *         Memory policy name 'sysMemPlc' is reserved for internal use.
 *     </li>
 *     <li>
 *         Memory policy size must be bigger than 1 MB (this rule will be changed as cache overhead is more than 1 MB
 *         and enforcing MemoryPolicy size at such low level doesn't prevent OutOfMemoryExceptions)
 *     </li>
 *     <li>
 *         If user provides a name for default memory policy, it cannot be empty.
 *     </li>
 *     <li>
 *         If user provides a name for default memory policy, this policy must be presented in the list.
 *     </li>
 * </ul>
 *
 * <p>Using XML configuration it can be configured as follows:</p>
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
 *                         <property name="size" value="#{1024 * 1024 * 1024}"/>
 *                     </bean>
 *                     <bean class="org.apache.ignite.configuration.MemoryPolicyConfiguration">
 *                         ...
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

    /** Default MemoryPolicy size is 1GB. */
    public static final long DFLT_MEMORY_POLICY_SIZE = 1024 * 1024 * 1024;

    /** Default size of memory chunk for system cache is 100MB. */
    public static final long DFLT_SYS_CACHE_MEM_SIZE = 100 * 1024 * 1024;

    /** Default page size. */
    public static final int DFLT_PAGE_SIZE = 2 * 1024;

    /** Size of memory for system cache. */
    private long sysCacheMemSize = DFLT_SYS_CACHE_MEM_SIZE;

    /** Page size. */
    private int pageSize = DFLT_PAGE_SIZE;

    /** Concurrency level. */
    private int concLvl;

    /** Name of MemoryPolicy to be used as default. */
    private String dfltMemPlcName;

    /** Memory policies. */
    private MemoryPolicyConfiguration[] memPlcs;

    /**
     * @return memory size for system cache.
     */
    public long getSystemCacheMemorySize() {
        return sysCacheMemSize;
    }

    /**
     * @param sysCacheMemSize Memory size for system cache.
     */
    public MemoryConfiguration setSystemCacheMemorySize(long sysCacheMemSize) {
        this.sysCacheMemSize = sysCacheMemSize;

        return this;
    }

    /**
     * @return Page size.
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * @param pageSize Page size.
     */
    public MemoryConfiguration setPageSize(int pageSize) {
        A.ensure(pageSize >= 1024 && pageSize <= 16 * 1024, "Page size must be between 1kB and 16kB.");
        A.ensure(U.isPow2(pageSize), "Page size must be a power of 2.");

        this.pageSize = pageSize;

        return this;
    }

    /**
     * @return array of MemoryPolicyConfiguration objects.
     */
    public MemoryPolicyConfiguration[] getMemoryPolicies() {
        return memPlcs;
    }

    /**
     * @param memPlcs MemoryPolicyConfiguration instances.
     */
    public MemoryConfiguration setMemoryPolicies(MemoryPolicyConfiguration... memPlcs) {
        this.memPlcs = memPlcs;

        return this;
    }

    /**
     * @return default {@link MemoryPolicyConfiguration} instance.
     */
    public MemoryPolicyConfiguration createDefaultPolicyConfig() {
        MemoryPolicyConfiguration memPlc = new MemoryPolicyConfiguration();

        memPlc.setName(null);
        memPlc.setSize(DFLT_MEMORY_POLICY_SIZE);

        return memPlc;
    }

    /**
     * @return Concurrency level.
     */
    public int getConcurrencyLevel() {
        return concLvl;
    }

    /**
     * @param concLvl Concurrency level.
     */
    public MemoryConfiguration setConcurrencyLevel(int concLvl) {
        this.concLvl = concLvl;

        return this;
    }

    /**
     * @return Name of MemoryPolicy to be used as default.
     */
    public String getDefaultMemoryPolicyName() {
        return dfltMemPlcName;
    }

    /**
     * @param dfltMemPlcName Name of MemoryPolicy to be used as default.
     */
    public MemoryConfiguration setDefaultMemoryPolicyName(String dfltMemPlcName) {
        this.dfltMemPlcName = dfltMemPlcName;

        return this;
    }
}
