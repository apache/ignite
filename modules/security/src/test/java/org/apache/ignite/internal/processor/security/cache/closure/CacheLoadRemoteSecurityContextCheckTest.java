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

package org.apache.ignite.internal.processor.security.cache.closure;

import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processor.security.AbstractCacheOperationRemoteSecurityContextCheckTest;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Testing permissions when the filter of Load cache is executed cache operations on remote node.
 */
@RunWith(JUnit4.class)
public class CacheLoadRemoteSecurityContextCheckTest extends AbstractCacheOperationRemoteSecurityContextCheckTest {
    /** Transition load cache. */
    private static final String TRANSITION_LOAD_CACHE = "TRANSITION_LOAD_CACHE";

    /** {@inheritDoc} */
    @Override protected CacheConfiguration[] getCacheConfigurations() {
        return new CacheConfiguration[] {
            new CacheConfiguration<Integer, Integer>()
                .setName(CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setReadFromBackup(false)
                .setCacheStoreFactory(new TestStoreFactory()),
            new CacheConfiguration<Integer, Integer>()
                .setName(TRANSITION_LOAD_CACHE)
                .setCacheMode(CacheMode.PARTITIONED)
                .setReadFromBackup(false)
                .setCacheStoreFactory(new TestStoreFactory())
        };
    }

    /**
     *
     */
    @Test
    public void test() {
        IgniteEx srvInitiator = grid("srv_initiator");

        IgniteEx clntInitiator = grid("clnt_initiator");

        perform(srvInitiator, ()->loadCache(srvInitiator));
        perform(clntInitiator, ()->loadCache(clntInitiator));
    }

    /**
     * @param initiator Initiator node.
     */
    private void loadCache(IgniteEx initiator) {
        initiator.<Integer, Integer>cache(CACHE_NAME).loadCache(
            new TestClosure("srv_transition", "srv_endpoint")
        );
    }

    /**
     * Closure for tests.
     */
    static class TestClosure implements IgniteBiPredicate<Integer, Integer> {
        /** Locale ignite. */
        @IgniteInstanceResource
        private Ignite loc;

        /** Expected local node name. */
        private final String node;

        /** Endpoint node name. */
        private final String endpoint;

        /**
         * @param node Expected local node name.
         * @param endpoint Endpoint node name.
         */
        public TestClosure(String node, String endpoint) {
            this.node = node;
            this.endpoint = endpoint;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(Integer k, Integer v) {
            if (node.equals(loc.name())) {
                VERIFIER.verify(loc);

                if (endpoint != null) {
                    loc.<Integer, Integer>cache(TRANSITION_LOAD_CACHE).loadCache(
                        new TestClosure(endpoint, null)
                    );
                }
            }

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
