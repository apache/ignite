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

package org.apache.ignite.internal.management.cache;

import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.jdbc.CacheAbstractJdbcStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactClass;

/**
 * Data transfer object for cache store configuration properties.
 */
public class CacheStoreConfiguration extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Whether cache has JDBC store. */
    @Order(value = 0)
    boolean jdbcStore;

    /** Cache store class name. */
    @Order(value = 1)
    String store;

    /** Cache store factory class name. */
    @Order(value = 2)
    String storeFactory;

    /** Whether cache should operate in read-through mode. */
    @Order(value = 3)
    boolean readThrough;

    /** Whether cache should operate in write-through mode. */
    @Order(value = 4)
    boolean writeThrough;

    /** Flag indicating whether write-behind behaviour should be used for the cache store. */
    @Order(value = 5)
    boolean writeBehindEnabled;

    /** Maximum batch size for write-behind cache store operations. */
    @Order(value = 6)
    int batchSz;

    /** Frequency with which write-behind cache is flushed to the cache store in milliseconds. */
    @Order(value = 7)
    long flushFreq;

    /** Maximum object count in write-behind cache. */
    @Order(value = 8)
    int flushSz;

    /** Number of threads that will perform cache flushing. */
    @Order(value = 9)
    int flushThreadCnt;

    /** Keep binary in store flag. */
    @Order(value = 10)
    boolean storeKeepBinary;

    /** Write coalescing flag for write-behind cache store */
    @Order(value = 11)
    boolean writeBehindCoalescing;

    /**
     * Default constructor.
     */
    public CacheStoreConfiguration() {
        // No-op.
    }

    /**
     * Create data transfer object for cache store configuration properties.
     *
     * @param ignite Ignite instance.
     * @param ccfg Cache configuration.
     */
    public CacheStoreConfiguration(IgniteEx ignite, CacheConfiguration ccfg) {
        IgniteCacheProxy<Object, Object> c = ignite.context().cache().jcache(ccfg.getName());

        CacheStore cstore = c != null && c.context().started() ? c.context().store().configuredStore() : null;

        jdbcStore = cstore instanceof CacheAbstractJdbcStore;

        store = compactClass(cstore);
        storeFactory = compactClass(ccfg.getCacheStoreFactory());

        readThrough = ccfg.isReadThrough();
        writeThrough = ccfg.isWriteThrough();

        writeBehindEnabled = ccfg.isWriteBehindEnabled();
        batchSz = ccfg.getWriteBehindBatchSize();
        flushFreq = ccfg.getWriteBehindFlushFrequency();
        flushSz = ccfg.getWriteBehindFlushSize();
        flushThreadCnt = ccfg.getWriteBehindFlushThreadCount();

        storeKeepBinary = ccfg.isStoreKeepBinary();

        writeBehindCoalescing = ccfg.getWriteBehindCoalescing();
    }

    /**
     * @return {@code true} if cache has store.
     */
    public boolean isEnabled() {
        return store != null;
    }

    /**
     * @return {@code true} if cache has JDBC store.
     */
    public boolean isJdbcStore() {
        return jdbcStore;
    }

    /**
     * @return Cache store class name.
     */
    @Nullable public String getStore() {
        return store;
    }

    /**
     * @return Cache store factory class name..
     */
    public String getStoreFactory() {
        return storeFactory;
    }

    /**
     * @return Whether cache should operate in read-through mode.
     */
    public boolean isReadThrough() {
        return readThrough;
    }

    /**
     * @return Whether cache should operate in write-through mode.
     */
    public boolean isWriteThrough() {
        return writeThrough;
    }

    /**
     * @return Flag indicating whether write-behind behaviour should be used for the cache store.
     */
    public boolean isWriteBehindEnabled() {
        return writeBehindEnabled;
    }

    /**
     * @return Maximum batch size for write-behind cache store operations.
     */
    public int getBatchSize() {
        return batchSz;
    }

    /**
     * @return Frequency with which write-behind cache is flushed to the cache store in milliseconds.
     */
    public long getFlushFrequency() {
        return flushFreq;
    }

    /**
     * @return Maximum object count in write-behind cache.
     */
    public int getFlushSize() {
        return flushSz;
    }

    /**
     * @return Number of threads that will perform cache flushing.
     */
    public int getFlushThreadCount() {
        return flushThreadCnt;
    }

    /**
     * @return Keep binary in store flag.
     */
    public boolean isStoreKeepBinary() {
        return storeKeepBinary;
    }

    /**
     * @return Write coalescing flag.
     */
    public boolean getWriteBehindCoalescing() {
        return writeBehindCoalescing;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheStoreConfiguration.class, this);
    }
}
