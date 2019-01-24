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
import org.apache.ignite.internal.util.typedef.T2;
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
public class LoadCacheSecurityTest extends AbstractCacheSecurityTest {
    /** Transition load cache. */
    private static final String TRANSITION_LOAD_CACHE = "TRANSITION_LOAD_CACHE";

    /** {@inheritDoc} */
    @Override protected CacheConfiguration[] getCacheConfigurations() {
        return new CacheConfiguration[] {
            new CacheConfiguration<String, Integer>()
                .setName(CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setReadFromBackup(false),
            new CacheConfiguration<Integer, Integer>()
                .setName(COMMON_USE_CACHE)
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
    public void testLoadCache() {
        testLoadCache(false);
        testLoadCache(true);
    }

    /**
     * @param isTransition True for transition case.
     */
    private void testLoadCache(boolean isTransition) {
        assertAllowed((t) -> load(clntAllPerms, closure(srvAllPerms, t, isTransition)));
        assertAllowed((t) -> load(clntAllPerms, closure(srvReadOnlyPerm, t, isTransition)));
        assertAllowed((t) -> load(srvAllPerms, closure(srvAllPerms, t, isTransition)));
        assertAllowed((t) -> load(srvAllPerms, closure(srvReadOnlyPerm, t, isTransition)));

        assertForbidden((t) -> load(clntReadOnlyPerm, closure(srvAllPerms, t, isTransition)));
        assertForbidden((t) -> load(srvReadOnlyPerm, closure(srvAllPerms, t, isTransition)));
        assertForbidden((t) -> load(srvReadOnlyPerm, closure(srvReadOnlyPerm, t, isTransition)));
    }

    /**
     * @param remote Remote.
     * @param entry Entry to put into test cache.
     * @param isTransition True if predicate to test transition case.
     */
    private IgniteBiPredicate<Integer, Integer> closure(IgniteEx remote,
        T2<String, Integer> entry, boolean isTransition) {
        assert !remote.localNode().isClient();

        if (isTransition)
            return new TransitionTestClosure(srvTransitionAllPerms.localNode().id(),
                remote.localNode().id(), entry);
        return new TestClosure(remote.localNode().id(), entry);
    }

    /**
     * @param initiator Initiator node.
     * @param p Predicate.
     */
    private void load(IgniteEx initiator, IgniteBiPredicate<Integer, Integer> p) {
        initiator.<Integer, Integer>cache(COMMON_USE_CACHE).loadCache(p);
    }

    /**
     * Closure for tests.
     */
    static class TestClosure implements IgniteBiPredicate<Integer, Integer> {
        /** Remote node id. */
        protected final UUID remoteId;

        /** Data to put into test cache. */
        protected final T2<String, Integer> t2;

        /** Locale ignite. */
        @IgniteInstanceResource
        protected Ignite loc;

        /**
         * @param remoteId Remote node id.
         * @param t2 Data to put into test cache.
         */
        public TestClosure(UUID remoteId, T2<String, Integer> t2) {
            this.remoteId = remoteId;
            this.t2 = t2;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(Integer k, Integer v) {
            if (remoteId.equals(loc.cluster().localNode().id()))
                loc.cache(CACHE_NAME).put(t2.getKey(), t2.getValue());

            return false;
        }
    }

    /**
     * Closure for transition tests.
     */
    static class TransitionTestClosure extends TestClosure {
        /** Transition node id. */
        private final UUID transitionId;

        /**
         * @param transitionId Transition node id.
         * @param remoteId Remote node id.
         * @param t2 Data to put into test cache.
         */
        public TransitionTestClosure(UUID transitionId, UUID remoteId, T2<String, Integer> t2) {
            super(remoteId, t2);

            this.transitionId = transitionId;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(Integer k, Integer v) {
            if (transitionId.equals(loc.cluster().localNode().id())) {
                loc.<Integer, Integer>cache(TRANSITION_LOAD_CACHE).loadCache(
                    new TestClosure(remoteId, t2)
                );
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
