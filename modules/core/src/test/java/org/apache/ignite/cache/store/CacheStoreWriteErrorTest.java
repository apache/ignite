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
import com.google.common.base.Throwables;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

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
     * Checks primary error while saving batch with one entry.
     */
    @Test
    public void testPrimaryErrorForBatchSize1() {
        checkPrimaryError(1);
    }

    /**
     * Checks primary error while saving batch with two entries.
     */
    @Test
    public void testPrimaryErrorForBatchSize2() {
        checkPrimaryError(2);
    }

    /**
     * Checks that primary error ({@link CacheWriterException}) is not lost due to unwrapping a key.
     *
     * @param batchSize Batch size.
     */
    private void checkPrimaryError(int batchSize) {
        Throwable t = GridTestUtils.assertThrows(log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    try (Ignite grid = startGrid()) {
                        IgniteCache<BinaryObject, String> cache = grid.cache(CACHE_NAME);

                        HashMap<BinaryObject, String> batch = new HashMap<>();

                        for (int i = 0; i < batchSize; i++) {
                            BinaryObject key = grid.binary().builder("KEY_TYPE_NAME").setField("id", i).build();

                            batch.put(key, "VALUE");
                        }

                        cache.putAllAsync(batch).get();
                    }

                    return null;
                }
            }, CacheWriterException.class, null);

        assertTrue("Stacktrace should contain the message of the original exception",
            Throwables.getStackTraceAsString(t).contains(ThrowableCacheStore.EXCEPTION_MESSAGE));
    }

    /**
     * {@link CacheStore} implementation which throws {@link RuntimeException} for every write operation.
     */
    public static class ThrowableCacheStore extends CacheStoreAdapter<Object, Object> {
        /** */
        private static final String EXCEPTION_MESSAGE = "WRITE CACHE STORE EXCEPTION";

        /** {@inheritDoc} */
        @Override public Object load(Object o) throws CacheLoaderException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<?, ?> entry) throws CacheWriterException {
            throw new RuntimeException(EXCEPTION_MESSAGE);
        }

        /** {@inheritDoc} */
        @Override public void delete(Object o) throws CacheWriterException {
            // No-op.
        }
    }
}
