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

import java.util.Collection;
import java.util.UUID;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processor.security.AbstractCacheOperationRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Testing operation security context when the filter of Load cache is executed on remote node.
 * <p>
 * The initiator node broadcasts a task to feature call node that starts load cache with filter. That filter is
 * executed on feature transition node and broadcasts a task to endpoint nodes. On every step, it is performed
 * verification that operation security context is the initiator context.
 */
@RunWith(JUnit4.class)
public class CacheLoadRemoteSecurityContextCheckTest extends AbstractCacheOperationRemoteSecurityContextCheckTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(SRV_INITIATOR, allowAllPermissionSet());

        startClient(CLNT_INITIATOR, allowAllPermissionSet());

        startGrid(SRV_FEATURE_CALL, allowAllPermissionSet());

        startGrid(SRV_FEATURE_TRANSITION, allowAllPermissionSet());

        startGrid(SRV_ENDPOINT, allowAllPermissionSet());

        startClient(CLNT_ENDPOINT, allowAllPermissionSet());

        G.allGrids().get(0).cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void setupVerifier(Verifier verifier) {
        verifier
            .add(SRV_FEATURE_CALL, 1)
            .add(SRV_FEATURE_TRANSITION, 1)
            .add(SRV_ENDPOINT, 1)
            .add(CLNT_ENDPOINT, 1);
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration[] getCacheConfigurations() {
        return new CacheConfiguration[] {
            new CacheConfiguration<Integer, Integer>()
                .setName(CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setCacheStoreFactory(new TestStoreFactory()),
            new CacheConfiguration<Integer, Integer>()
                .setName(TRANSITION_LOAD_CACHE)
                .setCacheMode(CacheMode.PARTITIONED)
                .setCacheStoreFactory(new TestStoreFactory())
        };
    }

    /**
     *
     */
    @Test
    public void test() {
        runAndCheck(SRV_INITIATOR);
        runAndCheck(CLNT_INITIATOR);
    }

    /**
     * @param name Initiator node name.
     */
    private void runAndCheck(String name) {
        runAndCheck(
            secSubjectId(name),
            () -> compute(grid(name), nodeId(SRV_FEATURE_CALL)).broadcast(
                () -> {
                    register();

                    loadCache(Ignition.localIgnite());
                }
            )
        );
    }

    /**
     * @param node Node.
     */
    private void loadCache(Ignite node) {
        node.<Integer, Integer>cache(CACHE_NAME).loadCache(
            new TestClosure(SRV_FEATURE_TRANSITION, endpoints())
        );
    }

    /**
     * Closure for tests.
     */
    static class TestClosure implements IgniteBiPredicate<Integer, Integer> {
        /** Local ignite. */
        @IgniteInstanceResource
        private Ignite loc;

        /** Expected local node name. */
        private final String node;

        /** Endpoint node id. */
        private final Collection<UUID> endpoints;

        /**
         * @param node Expected local node name.
         * @param endpoints Collection of endpont nodes ids.
         */
        public TestClosure(String node, Collection<UUID> endpoints) {
            assert node != null;
            assert !endpoints.isEmpty();

            this.node = node;
            this.endpoints = endpoints;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(Integer k, Integer v) {
            if (node.equals(loc.name())) {
                register();

                compute(loc, endpoints)
                    .broadcast(() -> register());
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
