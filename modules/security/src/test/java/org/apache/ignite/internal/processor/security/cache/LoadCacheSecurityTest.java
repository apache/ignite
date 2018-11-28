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

package org.apache.ignite.internal.processor.security.cache;

import java.util.UUID;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processor.security.AbstractCacheSecurityTest;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * Security tests for cache data load.
 */
public class LoadCacheSecurityTest extends AbstractCacheSecurityTest {
    /** {@inheritDoc} */
    @Override protected CacheConfiguration[] getCacheConfigurations() {
        return new CacheConfiguration[] {
            new CacheConfiguration<String, Integer>()
                .setName(CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setReadFromBackup(false),
            new CacheConfiguration<Integer, Integer>()
                .setName(CACHE_WITHOUT_PERMS)
                .setCacheMode(CacheMode.PARTITIONED)
                .setReadFromBackup(false)
                .setCacheStoreFactory(new TestStoreFactory())
        };
    }

    /**
     *
     */
    public void testLoadCache() {
        assertAllowed(() -> load(clntAllPerms, srvAllPerms, "key"));
        assertAllowed(() -> load(clntAllPerms, srvReadOnlyPerm, "key"));
        assertAllowed(() -> load(srvAllPerms, srvAllPerms, "key"));
        assertAllowed(() -> load(srvAllPerms, srvReadOnlyPerm, "key"));

        assertForbidden(() -> load(clntReadOnlyPerm, srvAllPerms, "fail_key"));
        assertForbidden(() -> load(srvReadOnlyPerm, srvAllPerms, "fail_key"));
        assertForbidden(() -> load(srvReadOnlyPerm, srvReadOnlyPerm, "fail_key"));
    }

    /**
     * @param initiator Initiator node.
     * @param remote Remoute node.
     * @param key Key.
     * @return Value that will be to put into cache with passed key.
     */
    private Integer load(IgniteEx initiator, IgniteEx remote, String key) {
        assert !remote.localNode().isClient();

        Integer val = values.getAndIncrement();

        initiator.<Integer, Integer>cache(CACHE_WITHOUT_PERMS).loadCache(
            new TestClosure(remote.localNode().id(), key, val)
        );

        return val;
    }

    /**
     * Closure for tests.
     */
    static class TestClosure implements IgniteBiPredicate<Integer, Integer> {
        /** Remote node id. */
        private final UUID remoteId;

        /** Key. */
        private final String key;

        /** Value. */
        private final Integer val;

        /** Locale ignite. */
        @IgniteInstanceResource
        protected Ignite loc;

        /**
         * @param remoteId Remote id.
         * @param key Key.
         * @param val Value.
         */
        public TestClosure(UUID remoteId, String key, Integer val) {
            this.remoteId = remoteId;
            this.key = key;
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(Integer k, Integer v) {
            if (remoteId.equals(loc.cluster().localNode().id()))
                loc.cache(CACHE_NAME).put(key, val);

            return false;
        }
    }

    /**
     * Test store factory.
     */
    private static class TestStoreFactory implements Factory<TestCacheStore> {
        /** {@inheritDoc} */
        @Override public TestCacheStore create() {
            return new TestCacheStore();
        }
    }

    /**
     * Test cache store.
     */
    private static class TestCacheStore extends CacheStoreAdapter<Integer, Integer> {
        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Integer, Integer> clo, Object... args) {
            clo.apply(1, 1);
        }

        /** {@inheritDoc} */
        @Override public Integer load(Integer key) throws CacheLoaderException {
            return key;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<? extends Integer, ? extends Integer> entry) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) {
            // No-op.
        }
    }
}
