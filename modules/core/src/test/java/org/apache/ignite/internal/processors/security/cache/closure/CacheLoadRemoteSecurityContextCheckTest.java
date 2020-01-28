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

package org.apache.ignite.internal.processors.security.cache.closure;

import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.security.AbstractCacheOperationRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.util.typedef.G;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.Ignition.localIgnite;

/**
 * Testing operation security context when the filter of Load cache is executed on remote node.
 * <p>
 * The initiator node broadcasts a task to 'run' node that starts load cache with filter. That filter is
 * executed on 'check' node and broadcasts a task to 'endpoint' nodes. On every step, it is performed
 * verification that operation security context is the initiator context.
 */
@RunWith(JUnit4.class)
public class CacheLoadRemoteSecurityContextCheckTest extends AbstractCacheOperationRemoteSecurityContextCheckTest {
    /** Transition load cache. */
    private static final String TRANSITION_LOAD_CACHE = "TRANSITION_LOAD_CACHE";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridAllowAll(SRV_INITIATOR);

        startClientAllowAll(CLNT_INITIATOR);

        startGridAllowAll(SRV_RUN);

        startGridAllowAll(SRV_CHECK);

        startGridAllowAll(SRV_ENDPOINT);

        startClientAllowAll(CLNT_ENDPOINT);

        G.allGrids().get(0).cluster().state(ClusterState.ACTIVE);
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration[] getCacheConfigurations() {
        return new CacheConfiguration[] {
            new CacheConfiguration<Integer, Integer>()
                .setName(CACHE_NAME)
                .setCacheMode(CacheMode.PARTITIONED)
                .setCacheStoreFactory(new TestStoreFactory(1, 1)),
            new CacheConfiguration<Integer, Integer>()
                .setName(TRANSITION_LOAD_CACHE)
                .setCacheMode(CacheMode.PARTITIONED)
                .setCacheStoreFactory(new TestStoreFactory(1, 1))
        };
    }

    /** */
    @Test
    public void test() {
        runAndCheck(() ->
            localIgnite().<Integer, Integer>cache(CACHE_NAME).loadCache(operationCheck(SRV_CHECK)));
    }

    /** {@inheritDoc} */
    @Override protected Collection<String> nodesToRun() {
        return Collections.singletonList(SRV_RUN);
    }

    /** {@inheritDoc} */
    @Override protected Collection<String> nodesToCheck() {
        return Collections.singletonList(SRV_CHECK);
    }
}
