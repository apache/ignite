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
 * Database configuration used to configure database and manage offheap memory of Ignite Node.
 *
 * <p>It may be configured under {@link IgniteConfiguration XML configuration} as follows:</p>
 * <pre>
 *     {@code
 *     <property name="memoryConfiguration">
 *         <bean class="org.apache.ignite.configuration.MemoryConfiguration">
 *             <property name="systemCacheMemorySize" value="103833600"/>
 *             <property name="defaultMemoryPolicySize" value="1063256064"/>
 *
 *             <property name="memoryPolicies">
 *                 <list>
 *                     <bean class="org.apache.ignite.configuration.MemoryPolicyConfiguration">
 *                         <property name="default" value="false"/>
 *                         <property name="name" value="operational_mem_plc"/>
 *                         <property name="size" value="103833600"/>
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

    /** Default cache size is 1GB. */
    public static final long DFLT_PAGE_CACHE_SIZE = 1024 * 1024 * 1024;

    /** Default size of memory chunk for system cache is 100MB. */
    public static final long DFLT_SYS_CACHE_MEM_SIZE = 100 * 1024 * 1024;

    /** Default page size. */
    public static final int DFLT_PAGE_SIZE = 2 * 1024;

    /** Size of memory for system cache. */
    private long sysCacheMemSize = DFLT_SYS_CACHE_MEM_SIZE;

    /** Size of memory for caches with default MemoryPolicy. */
    private long dfltMemPlcSize = DFLT_PAGE_CACHE_SIZE;

    /** Page size. */
    private int pageSize = DFLT_PAGE_SIZE;

    /** Concurrency level. */
    private int concLvl;

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
    public void setSystemCacheMemorySize(long sysCacheMemSize) {
        this.sysCacheMemSize = sysCacheMemSize;
    }

    /**
     * @return size in bytes of MemoryPolicy used by default.
     */
    public long getDefaultMemoryPolicySize() {
        return dfltMemPlcSize;
    }

    /**
     * @param dfltMemPlcSize Size of default memory policy.
     */
    public void setDefaultMemoryPolicySize(long dfltMemPlcSize) {
        this.dfltMemPlcSize = dfltMemPlcSize;
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
    public void setPageSize(int pageSize) {
        A.ensure(pageSize >= 1024 && pageSize <= 16 * 1024, "Page size must be between 1kB and 16kB.");
        A.ensure(U.isPow2(pageSize), "Page size must be a power of 2.");

        this.pageSize = pageSize;
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
    public void setMemoryPolicies(MemoryPolicyConfiguration... memPlcs) {
        this.memPlcs = memPlcs;
    }

    /**
     * @return default {@link MemoryPolicyConfiguration} instance.
     */
    public MemoryPolicyConfiguration createDefaultPolicy() {
        MemoryPolicyConfiguration memPlc = new MemoryPolicyConfiguration();

        memPlc.setDefault(true);
        memPlc.setName(null);
        memPlc.setSize(dfltMemPlcSize);

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
    public void setConcurrencyLevel(int concLvl) {
        this.concLvl = concLvl;
    }
}
