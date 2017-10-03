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
 * architecture that divides all expandable data regions into pages of fixed size
 * (see {@link DataStorageConfiguration#getPageSize()}. An individual page can store one or many cache key-value entries
 * that allows reusing the memory in the most efficient way and avoid memory fragmentation issues.
 * <p>
 * By default, the page memory allocates a single expandable data region using settings of
 * {@link DataStorageConfiguration#createDefaultRegionConfig()}. All the caches that will be configured in an application
 * will be mapped to this data region by default, thus, all the cache data will reside in that data region.
 * <p>
 * If initial size of the default data region doesn't satisfy requirements or it's required to have multiple data
 * regions with different properties then {@link DataRegionConfiguration} can be used for both scenarios.
 * For instance, using data regions you can define data regions of different maximum size, eviction policies,
 * swapping options, etc. Once you define a new data region you can bind particular Ignite caches to it.
 * <p>
 * To learn more about data regions refer to {@link DataRegionConfiguration} documentation.
 * <p>Sample configuration below shows how to make 5 GB data regions the default one for Apache Ignite:</p>
 * <pre>
 *     {@code
 *     <property name="dataStorageConfiguration">
 *         <bean class="org.apache.ignite.configuration.DataStorageConfiguration">
 *             <property name="systemCacheInitialSize" value="#{100 * 1024 * 1024}"/>
 *             <property name="defaultDataRegionName" value="default_data_region"/>
 *
 *             <property name="dataRegions">
 *                 <list>
 *                     <bean class="org.apache.ignite.configuration.DataRegionConfiguration">
 *                         <property name="name" value="default_data_region"/>
 *                         <property name="initialSize" value="#{5 * 1024 * 1024 * 1024}"/>
 *                     </bean>
 *                 </list>
 *             </property>
 *         </bean>
 *     </property>
 *     }
 * </pre>
 */
public class DataStorageConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default data region start size (256 MB). */
    @SuppressWarnings("UnnecessaryBoxing")
    public static final long DFLT_DATA_REGION_INITIAL_SIZE = 256L * 1024 * 1024;

    /** Fraction of available memory to allocate for default DataRegion. */
    private static final double DFLT_DATA_REGION_FRACTION = 0.2;

    /** Default data region's size is 20% of physical memory available on current machine. */
    public static final long DFLT_DATA_REGION_MAX_SIZE = Math.max(
        (long)(DFLT_DATA_REGION_FRACTION * U.getTotalMemoryAvailable()),
        DFLT_DATA_REGION_INITIAL_SIZE);

    /** Default initial size of a memory chunk for the system cache (40 MB). */
    private static final long DFLT_SYS_CACHE_INIT_SIZE = 40 * 1024 * 1024;

    /** Default max size of a memory chunk for the system cache (100 MB). */
    private static final long DFLT_SYS_CACHE_MAX_SIZE = 100 * 1024 * 1024;

    /** Default memory page size. */
    public static final int DFLT_PAGE_SIZE = 4 * 1024;

    /** This name is assigned to default Dataregion if no user-defined default MemPlc is specified */
    public static final String DFLT_DATA_REG_DEFAULT_NAME = "default";

    /** Size of a memory chunk reserved for system cache initially. */
    private long sysCacheInitSize = DFLT_SYS_CACHE_INIT_SIZE;

    /** Maximum size of system cache. */
    private long sysCacheMaxSize = DFLT_SYS_CACHE_MAX_SIZE;

    /** Memory page size. */
    private int pageSize;

    /** Concurrency level. */
    private int concLvl;

    /** A name of the data region that defines the default data region. */
    private String dfltDataRegName = DFLT_DATA_REG_DEFAULT_NAME;

    /** Size of memory (in bytes) to use for default data region. */
    private long dfltDataRegSize = DFLT_DATA_REGION_MAX_SIZE;

    /** Data regions. */
    private DataRegionConfiguration[] dataRegions;

    /**
     * Initial size of a data region reserved for system cache.
     *
     * @return Size in bytes.
     */
    public long getSystemCacheInitialSize() {
        return sysCacheInitSize;
    }

    /**
     * Sets initial size of a data region reserved for system cache.
     *
     * Default value is {@link #DFLT_SYS_CACHE_INIT_SIZE}
     *
     * @param sysCacheInitSize Size in bytes.
     *
     * @return {@code this} for chaining.
     */
    public DataStorageConfiguration setSystemCacheInitialSize(long sysCacheInitSize) {
        A.ensure(sysCacheMaxSize > 0, "System cache initial size can not be less zero.");

        this.sysCacheInitSize = sysCacheInitSize;

        return this;
    }

    /**
     * Maximum data region size reserved for system cache.
     *
     * @return Size in bytes.
     */
    public long getSystemCacheMaxSize() {
        return sysCacheMaxSize;
    }

    /**
     * Sets maximum data region size reserved for system cache. The total size should not be less than 10 MB
     * due to internal data structures overhead.
     *
     * @param sysCacheMaxSize Maximum size in bytes for system cache data region.
     *
     * @return {@code this} for chaining.
     */
    public DataStorageConfiguration setSystemCacheMaxSize(long sysCacheMaxSize) {
        A.ensure(sysCacheMaxSize > 0, "System cache max size can not be less zero.");

        this.sysCacheMaxSize = sysCacheMaxSize;

        return this;
    }

    /**
     * The page memory consists of one or more expandable data regions defined by {@link DataRegionConfiguration}.
     * Every data region is split on pages of fixed size that store actual cache entries.
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
    public DataStorageConfiguration setPageSize(int pageSize) {
        A.ensure(pageSize >= 1024 && pageSize <= 16 * 1024, "Page size must be between 1kB and 16kB.");
        A.ensure(U.isPow2(pageSize), "Page size must be a power of 2.");

        this.pageSize = pageSize;

        return this;
    }

    /**
     * Gets an array of all data regions configured. Apache Ignite will instantiate a dedicated data region per
     * region. An Apache Ignite cache can be mapped to a specific region with
     * {@link CacheConfiguration#setDataRegionName(String)} method.
     *
     * @return Array of configured data regions.
     */
    public DataRegionConfiguration[] getDataRegions() {
        return dataRegions;
    }

    /**
     * Sets data regions configurations.
     *
     * @param dataRegionConfigurations Data regions configurations.
     */
    public DataStorageConfiguration setDataRegions(DataRegionConfiguration... dataRegionConfigurations) {
        this.dataRegions = dataRegionConfigurations;

        return this;
    }

    /**
     * Creates a configuration for the default data region that will instantiate the default data region.
     * To override settings of the default data region in order to create the default data region with different
     * parameters, create own data region first, pass it to
     * {@link DataStorageConfiguration#setDataRegions(DataRegionConfiguration...)} method and change the name of the
     * default data region with {@link DataStorageConfiguration#setDefaultDataRegionName(String)}.
     *
     * @return default Data region configuration.
     */
    public DataRegionConfiguration createDefaultRegionConfig() {
        DataRegionConfiguration memPlc = new DataRegionConfiguration();

        long maxSize = dfltDataRegSize;

        if (maxSize < DFLT_DATA_REGION_INITIAL_SIZE)
            memPlc.setInitialSize(maxSize);
        else
            memPlc.setInitialSize(DFLT_DATA_REGION_INITIAL_SIZE);

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
    public DataStorageConfiguration setConcurrencyLevel(int concLvl) {
        this.concLvl = concLvl;

        return this;
    }

    /**
     * Gets a size for default data region overridden by user.
     *
     * @return Default data region size overridden by user or {@link #DFLT_DATA_REGION_MAX_SIZE} if nothing was specified.
     */
    public long getDefaultDataRegionSize() {
        return dfltDataRegSize;
    }

    /**
     * Overrides size of default data region which is created automatically.
     *
     * If user doesn't specify any data region configuration, a default one with default size
     * (80% of available RAM) is created by Ignite.
     *
     * This property allows user to specify desired size of default data region
     * without having to use more verbose syntax of DataRegionConfiguration elements.
     *
     * @param dfltMemPlcSize Size of default data region overridden by user.
     */
    public DataStorageConfiguration setDefaultDataRegionSize(long dfltMemPlcSize) {
        this.dfltDataRegSize = dfltMemPlcSize;

        return this;
    }

    /**
     * Gets a name of default data region.
     *
     * @return A name of a custom data region configured with {@code DataStorageConfiguration} or {@code null} of the
     *         default region is used.
     */
    public String getDefaultDataRegionName() {
        return dfltDataRegName;
    }

    /**
     * Sets the name for the default data region that will initialize the default data region.
     * To set own default data region, create the region first, pass it to
     * {@link DataStorageConfiguration#setDataRegions(DataRegionConfiguration...)} method and change the name of the
     * default data region with {@code DataStorageConfiguration#setDefaultDataRegionName(String)}.
     *
     * If nothing is specified by user, it is set to {@link #DFLT_DATA_REG_DEFAULT_NAME} value.
     *
     * @param dfltMemRegName Name of a data region to be used as default one.
     */
    public DataStorageConfiguration setDefaultDataRegionName(String dfltMemRegName) {
        this.dfltDataRegName = dfltMemRegName;

        return this;
    }
}
