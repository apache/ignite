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

import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

/**
 * Configuration for embedded indexing facilities.
 */
public class IgniteQueryConfiguration {
    /** Default query execution time interpreted as long query (3 seconds). */
    public static final long DFLT_LONG_QRY_EXEC_TIMEOUT = 3000;

    /** Default value for {@link #setUseOptimizedSerializer(boolean)} flag. */
    public static final boolean DFLT_USE_OPTIMIZED_SERIALIZER = true;

    /** */
    private Class<?>[] idxCustomFuncClss;

    /** */
    private String[] searchPath;

    /** */
    private String initScriptPath;

    /** */
    private long maxOffHeapMemory = -1;

    /** */
    private long longQryExecTimeout = DFLT_LONG_QRY_EXEC_TIMEOUT;

    /** */
    private boolean longQryExplain;

    /** */
    private boolean useOptimizedSerializer = DFLT_USE_OPTIMIZED_SERIALIZER;

    /**
     * Sets maximum amount of memory available to off-heap storage. Possible values are
     * <ul>
     * <li>{@code -1} - Means that off-heap storage is disabled.</li>
     * <li>
     *     {@code 0} - GridGain will not limit off-heap storage (it's up to user to properly
     *     add and remove entries from cache to ensure that off-heap storage does not grow
     *     indefinitely.
     * </li>
     * <li>Any positive value specifies the limit of off-heap storage in bytes.</li>
     * </ul>
     * Default value is {@code -1}, which means that off-heap storage is disabled by default.
     * <p>
     * Use off-heap storage to load gigabytes of data in memory without slowing down
     * Garbage Collection. Essentially in this case you should allocate very small amount
     * of memory to JVM and GridGain will cache most of the data in off-heap space
     * without affecting JVM performance at all.
     *
     * @param maxOffHeapMemory Maximum memory in bytes available to off-heap memory space.
     */
    public void setMaxOffHeapMemory(long maxOffHeapMemory) {
        this.maxOffHeapMemory = maxOffHeapMemory;
    }

    /** {@inheritDoc} */
    public long getMaxOffHeapMemory() {
        return maxOffHeapMemory;
    }

    /**
     * Specifies max allowed size of cache for deserialized offheap rows to avoid deserialization costs for most
     * frequently used ones. In general performance is better with greater cache size. Must be more than 128 items.
     *
     * @param size Cache size in items.
     */
    public void setMaxOffheapRowsCacheSize(int size) {
        A.ensure(size >= 128, "Offheap rows cache size must be not less than 128.");

//        rowCache = new CacheLongKeyLIRS<>(size, 1, 128, 256); TODO
    }

    /**
     * Sets the optional search path consisting of space names to search SQL schema objects. Useful for cross cache
     * queries to avoid writing fully qualified table names.
     *
     * @param searchPath Search path.
     */
    public void setSearchPath(String... searchPath) {
        this.searchPath = searchPath;
    }

    /** {@inheritDoc} */
    @Nullable public String[] getSearchPath() {
        return searchPath;
    }

    /** {@inheritDoc} */
    @Nullable public String getInitialScriptPath() {
        return initScriptPath;
    }

    /**
     * Sets script path to be ran against H2 database after opening.
     * The script must be UTF-8 encoded file.
     *
     * @param initScriptPath Script path.
     */
    public void setInitialScriptPath(String initScriptPath) {
        this.initScriptPath = initScriptPath;
    }

    /**
     * Sets classes with methods annotated by {@link org.apache.ignite.cache.query.CacheQuerySqlFunction}
     * to be used as user-defined functions from SQL queries.
     *
     * @param idxCustomFuncClss List of classes.
     */
    public void setIndexCustomFunctionClasses(Class<?>... idxCustomFuncClss) {
        this.idxCustomFuncClss = idxCustomFuncClss;
    }

    /** {@inheritDoc} */
    @Nullable public Class<?>[] getIndexCustomFunctionClasses() {
        return idxCustomFuncClss;
    }

    /** {@inheritDoc} */
    public long getLongQueryExecutionTimeout() {
        return longQryExecTimeout;
    }

    /**
     * Set query execution time threshold. If queries exceed this threshold,
     * then a warning will be printed out. If {@link #setLongQueryExplain(boolean)} is
     * set to {@code true}, then execution plan will be printed out as well.
     * <p>
     * If not provided, default value is defined by {@link #DFLT_LONG_QRY_EXEC_TIMEOUT}.
     *
     * @param longQryExecTimeout Long query execution timeout.
     * @see #setLongQueryExplain(boolean)
     */
    public void setLongQueryExecutionTimeout(long longQryExecTimeout) {
        this.longQryExecTimeout = longQryExecTimeout;
    }

    /** {@inheritDoc} */
    public boolean isLongQueryExplain() {
        return longQryExplain;
    }

    /**
     * If {@code true}, SPI will print SQL execution plan for long queries (explain SQL query).
     * The time threshold of long queries is controlled via {@link #setLongQueryExecutionTimeout(long)}
     * parameter.
     * <p>
     * If not provided, default value is {@code false}.
     *
     * @param longQryExplain Flag marking SPI should print SQL execution plan for long queries (explain SQL query).
     * @see #setLongQueryExecutionTimeout(long)
     */
    public void setLongQueryExplain(boolean longQryExplain) {
        this.longQryExplain = longQryExplain;
    }

    /**
     * The flag indicating that serializer for H2 database will be set to GridGain's marshaller.
     * This setting usually makes sense for offheap indexing only.
     * <p>
     * Default is {@link #DFLT_USE_OPTIMIZED_SERIALIZER}.
     *
     * @param useOptimizedSerializer Flag value.
     */
    public void setUseOptimizedSerializer(boolean useOptimizedSerializer) {
        this.useOptimizedSerializer = useOptimizedSerializer;
    }

    /**
     * The flag indicating that serializer for H2 database will be set to GridGain's marshaller.
     * This setting usually makes sense for offheap indexing only.
     * <p>
     * Default is {@link #DFLT_USE_OPTIMIZED_SERIALIZER}.
     *
     * @return Flag value.
     */
    public boolean isUseOptimizedSerializer() {
        return useOptimizedSerializer;
    }
}
