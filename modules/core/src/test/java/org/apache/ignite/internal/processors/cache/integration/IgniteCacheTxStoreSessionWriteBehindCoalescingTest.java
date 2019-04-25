/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.integration;

import java.util.Collection;
import javax.cache.Cache;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.junit.Before;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 * Integration test write behind cache store with {@link CacheConfiguration#getWriteBehindCoalescing()}={@code False}
 * parameter.
 */
public class IgniteCacheTxStoreSessionWriteBehindCoalescingTest extends IgniteCacheStoreSessionWriteBehindAbstractTest {
    /** */
    @Before
    public void beforeIgniteCacheTxStoreSessionWriteBehindCoalescingTest() {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.CACHE_STORE);
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /**
     * @param igniteInstanceName Ignite instance name.
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    @SuppressWarnings("unchecked")
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.CACHE_STORE);

        CacheConfiguration ccfg = super.cacheConfiguration(igniteInstanceName);

        ccfg.setWriteBehindCoalescing(false);

        ccfg.setCacheStoreFactory(singletonFactory(new TestNonCoalescingStore()));

        return ccfg;
    }

    /**
     *
     */
    private class TestNonCoalescingStore extends TestStore {

        /** {@inheritDoc} */
        @Override public void writeAll(Collection<Cache.Entry<?, ?>> entries) throws CacheWriterException {
            log.info("writeAll: " + entries);

            assertTrue("Unexpected entries: " + entries, entries.size() <= 10);

            checkSession("writeAll");

            for (int i = 0; i < entries.size(); i++)
                entLatch.countDown();
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            fail();
        }

        /** {@inheritDoc} */
        @Override public void deleteAll(Collection<?> keys) throws CacheWriterException {
            log.info("deleteAll: " + keys);

            assertTrue("Unexpected keys: " + keys, keys.size() <= 10);

            checkSession("deleteAll");

            for (int i = 0; i < keys.size(); i++)
                entLatch.countDown();
        }
    }
}
