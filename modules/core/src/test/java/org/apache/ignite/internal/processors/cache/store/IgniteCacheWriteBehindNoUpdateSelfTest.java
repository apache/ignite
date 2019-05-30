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

package org.apache.ignite.internal.processors.cache.store;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IgniteCacheWriteBehindNoUpdateSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String THROTTLES_CACHE_NAME = "test";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(1);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<String, Long> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setReadFromBackup(true);
        ccfg.setCopyOnRead(false);
        ccfg.setName(THROTTLES_CACHE_NAME);

        Duration expiryDuration = new Duration(TimeUnit.MINUTES, 1);

        ccfg.setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(expiryDuration));
        ccfg.setReadThrough(false);
        ccfg.setWriteThrough(true);

        ccfg.setCacheStoreFactory(new FactoryBuilder.SingletonFactory<>(new TestCacheStore()));

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEntryProcessorNoUpdate() throws Exception {
        IgniteCache<Object, Object> cache = ignite(0).cache(THROTTLES_CACHE_NAME);

        IgniteCache<Object, Object> skipStore = cache.withSkipStore();

        int entryCnt = 500;

        Set<String> keys = new HashSet<>();

        for (int i = 0; i < entryCnt; i++) {
            skipStore.put(String.valueOf(i), i);

            keys.add(String.valueOf(i));
        }

        TestCacheStore testStore = (TestCacheStore)grid(0).context().cache().cache(THROTTLES_CACHE_NAME).context()
            .store().configuredStore();

        assertEquals(0, testStore.writeCnt.get());

        cache.invokeAll(keys, new NoOpEntryProcessor());

        assertEquals(0, testStore.writeCnt.get());

        cache.invokeAll(keys, new OpEntryProcessor());

        assertEquals(1, testStore.writeCnt.get());
    }

    /**
     *
     */
    private static class TestCacheStore extends CacheStoreAdapter<String, Long> implements Serializable {
        /** */
        private AtomicInteger writeCnt = new AtomicInteger();

        /**
         *
         */
        public void resetWrites() {
            writeCnt.set(0);
        }

        /** {@inheritDoc} */
        @Override public Long load(String key) throws CacheLoaderException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void writeAll(Collection<Cache.Entry<? extends String, ? extends Long>> entries) {
            writeCnt.incrementAndGet();
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry entry) throws CacheWriterException {
            assert false;
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {

        }
    }

    /**
     *
     */
    private static class NoOpEntryProcessor implements EntryProcessor {
        /** {@inheritDoc} */
        @Override public Object process(MutableEntry entry, Object... arguments) throws EntryProcessorException {
            entry.getValue();

            return null;
        }
    }

    /**
     *
     */
    private static class OpEntryProcessor implements EntryProcessor {
        /** {@inheritDoc} */
        @Override public Object process(MutableEntry entry, Object... arguments) throws EntryProcessorException {
            entry.setValue(1L);

            return null;
        }
    }
}
