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

package org.apache.ignite.cache.store;

import java.util.HashMap;
import java.util.concurrent.Callable;
import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * This class tests handling exceptions from {@link CacheStore#write(Cache.Entry)}.
 */
public class CacheStoreWriteErrorTest extends GridCommonAbstractTest {
    /** */
    public static final String CACHE_NAME = "cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        CacheConfiguration cacheCfg = new CacheConfiguration(CACHE_NAME)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.PRIMARY_SYNC)
            .setCacheStoreFactory(FactoryBuilder.factoryOf(ThrowableCacheStore.class))
            .setWriteThrough(true)
            .setStoreKeepBinary(true);

        return super.getConfiguration(gridName)
            .setCacheConfiguration(cacheCfg);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Checks that primary error ({@link CacheWriterException}) is not lost due to unwrapping a key.
     */
    public void testPrimaryError() {
        GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    try (Ignite grid = startGrid()) {
                        IgniteCache<BinaryObject, String> cache = grid.cache(CACHE_NAME);

                        HashMap<BinaryObject, String> batch = new HashMap<>();

                        BinaryObject key = grid.binary().builder("KEY_TYPE_NAME").setField("id", 0).build();

                        batch.put(key, "VALUE");

                        cache.putAllAsync(batch).get();
                    }

                    return null;
                }
            }, CacheWriterException.class, null);
    }

    /**
     * {@link CacheStore} implementation which throws {@link RuntimeException} for every write operation.
     */
    public static class ThrowableCacheStore extends CacheStoreAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override public Object load(Object o) throws CacheLoaderException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<?, ?> entry) throws CacheWriterException {
            throw new RuntimeException();
        }

        /** {@inheritDoc} */
        @Override public void delete(Object o) throws CacheWriterException {
            // No-op.
        }
    }
}
