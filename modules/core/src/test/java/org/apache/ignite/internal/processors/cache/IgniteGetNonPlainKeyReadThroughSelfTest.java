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

package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests read through for non-{@link BinaryUtils#BINARY_CLS} keys.
 */
public class IgniteGetNonPlainKeyReadThroughSelfTest extends GridCommonAbstractTest {
    /** */
    private StoreFactory storeFactory;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(CacheMode.PARTITIONED);

        ccfg.setReadThrough(true);

        ccfg.setCacheStoreFactory(storeFactory);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    private static class StoreFactory implements Factory<CacheStore<Object, Object>> {
        private boolean nullVal;

        public StoreFactory(boolean nullVal) {
            this.nullVal = nullVal;
        }

        /** {@inheritDoc} */
        @Override public CacheStore<Object, Object> create() {
            if (nullVal)
                return new IgniteGetNonPlainKeyReadThroughSelfTest.Store(true);
            else
                return new IgniteGetNonPlainKeyReadThroughSelfTest.Store(false);
        }
    }

    /**
     *
     */
    private static class Store extends CacheStoreAdapter<Object, Object> implements Serializable {
        private boolean nullVal;

        public Store(boolean nullVal) {
            this.nullVal = nullVal;
        }

        /** {@inheritDoc} */
        @Override public Object load(Object key) throws CacheLoaderException {
            if (nullVal)
                return null;
            else
                return key;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry e)
            throws CacheWriterException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            // No-op.
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetNullRead() throws Exception {
        storeFactory = new StoreFactory(true);

        testGet(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetValueRead() throws Exception {
        storeFactory = new StoreFactory(false);

        testGet(false);
    }

    /**
     * @throws Exception If failed.
     */
    private void testGet(boolean nullRead) throws Exception {
        try {
            final Ignite ignite = startGrid();

            final GridCacheAdapter cache = ((IgniteKernal)grid()).internalCache(DEFAULT_CACHE_NAME);

            IgniteBiTuple<String, String> key = new IgniteBiTuple<>();

            if (nullRead) {
                assertEquals(null, cache.get("key"));
                assertEquals(null, cache.get(key));
            }
            else {
                assertEquals("key", cache.get("key"));
                assertEquals(key, cache.get(key));
            }
        }
        finally {
            stopAllGrids();
        }
    }
}
