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
import java.util.Collections;
import java.util.UUID;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processor.security.AbstractCacheOperationRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Testing operation security context when the filter of Load cache is executed on remote node.
 * <p>
 * The initiator node broadcasts a task to 'run' node that starts load cache with filter. That filter is
 * executed on 'check' node and broadcasts a task to 'endpoint' nodes. On every step, it is performed
 * verification that operation security context is the initiator context.
 */
@RunWith(JUnit4.class)
public class CacheLoadRemoteSecurityContextCheckTest extends AbstractCacheOperationRemoteSecurityContextCheckTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(SRV_INITIATOR, allowAllPermissionSet());

        startClient(CLNT_INITIATOR, allowAllPermissionSet());

        startGrid(SRV_RUN, allowAllPermissionSet());

        startGrid(SRV_CHECK, allowAllPermissionSet());

        startGrid(SRV_ENDPOINT, allowAllPermissionSet());

        startClient(CLNT_ENDPOINT, allowAllPermissionSet());

        G.allGrids().get(0).cluster().active(true);
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

    /** {@inheritDoc} */
    @Override protected void setupVerifier(Verifier verifier) {
        verifier
            .expect(SRV_RUN, 1)
            .expect(SRV_CHECK, 1)
            .expect(SRV_ENDPOINT, 1)
            .expect(CLNT_ENDPOINT, 1);
    }

    /**
     *
     */
    @Test
    public void test() {
        IgniteRunnable checkCase = () -> {
            register();

            Ignition.localIgnite()
                .<Integer, Integer>cache(CACHE_NAME).loadCache(
                new RegisterExecAndForward<Integer, Integer>(SRV_CHECK, endpoints())
            );
        };

        runAndCheck(grid(SRV_INITIATOR), checkCase);
        runAndCheck(grid(CLNT_INITIATOR), checkCase);
    }

    /** {@inheritDoc} */
    @Override protected Collection<UUID> nodesToRun() {
        return Collections.singletonList(nodeId(SRV_RUN));
    }

    /** {@inheritDoc} */
    @Override protected Collection<UUID> nodesToCheck() {
        return Collections.singletonList(nodeId(SRV_CHECK));
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
        @Override public Integer load(Integer key) {
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
