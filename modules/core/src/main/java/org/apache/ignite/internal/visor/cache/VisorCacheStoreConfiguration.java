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

package org.apache.ignite.internal.visor.cache;

import java.io.Serializable;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.jdbc.CacheAbstractJdbcStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.LessNamingBean;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.compactClass;

/**
 * Data transfer object for cache store configuration properties.
 */
public class VisorCacheStoreConfiguration implements Serializable, LessNamingBean {
    /** */
    private static final long serialVersionUID = 0L;

    /** Whether cache has JDBC store. */
    private boolean jdbcStore;

    /** Cache store class name. */
    private String store;

    /** Cache store factory class name. */
    private String storeFactory;

    /** Whether cache should operate in read-through mode. */
    private boolean readThrough;

    /** Whether cache should operate in write-through mode. */
    private boolean writeThrough;

    /** Flag indicating whether write-behind behaviour should be used for the cache store. */
    private boolean writeBehindEnabled;

    /** Maximum batch size for write-behind cache store operations. */
    private int batchSz;

    /** Frequency with which write-behind cache is flushed to the cache store in milliseconds. */
    private long flushFreq;

    /** Maximum object count in write-behind cache. */
    private int flushSz;

    /** Number of threads that will perform cache flushing. */
    private int flushThreadCnt;

    /** Keep binary in store flag. */
    private boolean storeKeepBinary;

    /**
     * @param ignite Ignite instance.
     * @param ccfg Cache configuration.
     * @return Data transfer object for cache store configuration properties.
     */
    public VisorCacheStoreConfiguration from(IgniteEx ignite, CacheConfiguration ccfg) {
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

        return this;
    }

    /**
     * @return {@code true} if cache has store.
     */
    public boolean enabled() {
        return store != null;
    }

    /**
     * @return {@code true} if cache has JDBC store.
     */
    public boolean jdbcStore() {
        return jdbcStore;
    }

    /**
     * @return Cache store class name.
     */
    @Nullable public String store() {
        return store;
    }

    /**
     * @return Cache store factory class name..
     */
    public String storeFactory() {
        return storeFactory;
    }

    /**
     * @return Whether cache should operate in read-through mode.
     */
    public boolean readThrough() {
        return readThrough;
    }

    /**
     * @return Whether cache should operate in write-through mode.
     */
    public boolean writeThrough() {
        return writeThrough;
    }

    /**
     * @return Flag indicating whether write-behind behaviour should be used for the cache store.
     */
    public boolean writeBehindEnabled() {
        return writeBehindEnabled;
    }

    /**
     * @return Maximum batch size for write-behind cache store operations.
     */
    public int batchSize() {
        return batchSz;
    }

    /**
     * @return Frequency with which write-behind cache is flushed to the cache store in milliseconds.
     */
    public long flushFrequency() {
        return flushFreq;
    }

    /**
     * @return Maximum object count in write-behind cache.
     */
    public int flushSize() {
        return flushSz;
    }

    /**
     * @return Number of threads that will perform cache flushing.
     */
    public int flushThreadCount() {
        return flushThreadCnt;
    }

    /**
     * @return Keep binary in store flag.
     */
    public boolean storeKeepBinary() {
        return storeKeepBinary;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheStoreConfiguration.class, this);
    }
}
