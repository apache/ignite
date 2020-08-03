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

package org.apache.ignite.internal.processors.database;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.IgniteReflectionFactory;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 *
 */
public class IgniteDbPutGetWithCacheStoreTest extends GridCommonAbstractTest {
    /** */
    private static Map<Object, Object> storeMap = new ConcurrentHashMap<>();

    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private CacheAtomicityMode atomicityMode;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        DataStorageConfiguration dbCfg = new DataStorageConfiguration();

        dbCfg
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setMaxSize(512L * 1024 * 1024)
                    .setPersistenceEnabled(true))
            .setWalMode(WALMode.LOG_ONLY);

        cfg.setDataStorageConfiguration(dbCfg);

        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(CACHE_NAME)
            .setCacheStoreFactory(new IgniteReflectionFactory<>(TestStore.class))
            .setAtomicityMode(atomicityMode)
            .setBackups(1)
            .setWriteThrough(true)
            .setReadThrough(true);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.CACHE_STORE);

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cleanPersistenceDir();

        storeMap.clear();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        cleanPersistenceDir();

        storeMap.clear();
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testWriteThrough() throws Exception {
        checkWriteThrough(ATOMIC);
        checkWriteThrough(TRANSACTIONAL);
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testReadThrough() throws Exception {
        checkReadThrough(ATOMIC);
        checkReadThrough(TRANSACTIONAL);
    }

    /**
     * @param atomicityMode Atomicity mode.
     * @throws Exception if failed.
     */
    private void checkWriteThrough(CacheAtomicityMode atomicityMode) throws Exception {
        this.atomicityMode = atomicityMode;

        IgniteEx ig = startGrid(0);

        try {
            ig.active(true);

            for (int i = 0; i < 2000; i++)
                ig.cache(CACHE_NAME).put(i, i);

            assertEquals(2000, storeMap.size());

            stopAllGrids(false);

            storeMap.clear();

            ig = startGrid(0);

            ig.active(true);

            for (int i = 0; i < 2000; i++)
                assertEquals(i, ig.cache(CACHE_NAME).get(i));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param atomicityMode Atomicity mode.
     * @throws Exception if failed.
     */
    private void checkReadThrough(CacheAtomicityMode atomicityMode) throws Exception {
        this.atomicityMode = atomicityMode;

        IgniteEx ig = startGrid(0);

        try {
            ig.active(true);

            for (int i = 0; i < 2000; i++)
                storeMap.put(i, i);

            for (int i = 0; i < 2000; i++)
                assertEquals(i, ig.cache(CACHE_NAME).get(i));

            stopAllGrids(false);

            storeMap.clear();

            ig = startGrid(0);

            ig.active(true);

            for (int i = 0; i < 2000; i++)
                assertEquals(i, ig.cache(CACHE_NAME).get(i));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    public static class TestStore extends CacheStoreAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override public Object load(Object key) throws CacheLoaderException {
            return storeMap.get(key);
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<?, ?> entry) throws CacheWriterException {
            storeMap.put(entry.getKey(), entry.getValue());
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            storeMap.remove(key);
        }
    }
}
