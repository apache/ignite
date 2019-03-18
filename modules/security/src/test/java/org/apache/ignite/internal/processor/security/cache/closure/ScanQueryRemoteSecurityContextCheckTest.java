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

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.internal.processor.security.AbstractCacheOperationRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Testing operation security context when the filter of ScanQuery is executed on remote node.
 * <p>
 * The initiator node broadcasts a task to feature call node that starts ScanQuery's filter. That filter is executed on
 * feature transition node and broadcasts a task to endpoint nodes. On every step, it is performed verification that
 * operation security context is the initiator context.
 */
@RunWith(JUnit4.class)
public class ScanQueryRemoteSecurityContextCheckTest extends AbstractCacheOperationRemoteSecurityContextCheckTest {
    /** Name of server initiator node. */
    private static final String SRV_INITIATOR = "srv_initiator";

    /** Name of client initiator node. */
    private static final String CLNT_INITIATOR = "clnt_initiator";

    /** Name of server feature call node. */
    private static final String SRV_FEATURE_CALL = "srv_feature_call";

    /** Name of server feature transit node. */
    private static final String SRV_FEATURE_TRANSITION = "srv_feature_transition";

    /** Name of client feature call node. */
    private static final String CLNT_FEATURE_CALL = "clnt_feature_call";

    /** Name of server endpoint node. */
    private static final String SRV_ENDPOINT = "srv_endpoint";

    /** Name of client endpoint node. */
    private static final String CLNT_ENDPOINT = "clnt_endpoint";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(SRV_INITIATOR, allowAllPermissionSet());

        startClient(CLNT_INITIATOR, allowAllPermissionSet());

        startGrid(SRV_FEATURE_CALL, allowAllPermissionSet());

        startClient(CLNT_FEATURE_CALL, allowAllPermissionSet());

        startGrid(SRV_FEATURE_TRANSITION, allowAllPermissionSet());

        startGrid(SRV_ENDPOINT, allowAllPermissionSet());

        startClient(CLNT_ENDPOINT, allowAllPermissionSet());

        G.allGrids().get(0).cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void setupVerifier(Verifier verifier) {
        verifier
            .add(SRV_FEATURE_CALL, 1)
            .add(CLNT_FEATURE_CALL, 1)
            .add(SRV_FEATURE_TRANSITION, 2)
            .add(SRV_ENDPOINT, 2)
            .add(CLNT_ENDPOINT, 2);
    }

    /**
     *
     */
    @Test
    public void test() throws Exception {
        grid(SRV_INITIATOR).cache(CACHE_NAME)
            .put(prmKey(grid(SRV_FEATURE_TRANSITION)), 1);

        awaitPartitionMapExchange();

        runAndCheck(SRV_INITIATOR);
        runAndCheck(CLNT_INITIATOR);
    }

    /**
     * @param name Inintiator node name.
     */
    private void runAndCheck(String name) {
        for (IgniteRunnable r : runnables()) {
            runAndCheck(secSubjectId(name),
                () -> compute(grid(name), featureCalls()).broadcast(r));
        }
    }

    /**
     *
     */
    private IgniteRunnable[] runnables() {
        return new IgniteRunnable[] {
            () -> {
                register();

                Ignition.localIgnite().cache(CACHE_NAME).query(
                    new ScanQuery<>(
                        new CommonClosure(SRV_FEATURE_TRANSITION, endpoints())
                    )
                ).getAll();
            },
            () -> {
                register();

                Ignition.localIgnite().cache(CACHE_NAME).query(
                    new ScanQuery<>((k, v) -> true),
                    new CommonClosure(SRV_FEATURE_TRANSITION, endpoints())
                ).getAll();
            }
        };
    }

    /**
     * @return Collection of feature call nodes ids.
     */
    private Collection<UUID> featureCalls() {
        return Arrays.asList(
            nodeId(SRV_FEATURE_CALL),
            nodeId(CLNT_FEATURE_CALL)
        );
    }

    /**
     * @return Collection of endpont nodes ids.
     */
    private Collection<UUID> endpoints() {
        return Arrays.asList(
            nodeId(SRV_ENDPOINT),
            nodeId(CLNT_ENDPOINT)
        );
    }

    /**
     * Common closure for tests.
     */
    static class CommonClosure implements IgniteClosure<Cache.Entry<Integer, Integer>, Integer>,
        IgniteBiPredicate<Integer, Integer> {
        /** Expected local node name. */
        private final String node;

        /** Collection of endpont nodes ids. */
        private final Collection<UUID> endpoints;

        /**
         * @param node Expected local node name.
         * @param endpoints Collection of endpont nodes ids.
         */
        public CommonClosure(String node, Collection<UUID> endpoints) {
            assert !endpoints.isEmpty();

            this.node = node;
            this.endpoints = endpoints;
        }

        /** {@inheritDoc} */
        @Override public Integer apply(Cache.Entry<Integer, Integer> entry) {
            verifyAndBroadcast();

            return entry.getValue();
        }

        /** {@inheritDoc} */
        @Override public boolean apply(Integer s, Integer i) {
            verifyAndBroadcast();

            return false;
        }

        /**
         *
         */
        private void verifyAndBroadcast() {
            Ignite loc = Ignition.localIgnite();

            if (node.equals(loc.name())) {
                register();

                compute(loc, endpoints)
                    .broadcast(() -> register());
            }
        }
    }
}
