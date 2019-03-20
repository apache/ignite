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

package org.apache.ignite.internal.processor.security.compute.closure;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processor.security.AbstractRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.junit.Test;

/**
 * Testing operation security context when the compute closure is executed on remote nodes.
 * <p>
 * The initiator node broadcasts a task to feature call nodes that starts compute operation. That operation is executed
 * on feature transition nodes and broadcasts a task to endpoint nodes. On every step, it is performed verification that
 * operation security context is the initiator context.
 */
public class DistributedClosureRemoteSecurityContextCheckTest
    extends AbstractRemoteSecurityContextCheckTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(SRV_INITIATOR, allowAllPermissionSet());

        startClient(CLNT_INITIATOR, allowAllPermissionSet());

        startGrid(SRV_FEATURE_CALL, allowAllPermissionSet());

        startClient(CLNT_FEATURE_CALL, allowAllPermissionSet());

        startGrid(SRV_FEATURE_TRANSITION, allowAllPermissionSet());

        startClient(CLNT_FEATURE_TRANSITION, allowAllPermissionSet());

        startGrid(SRV_ENDPOINT, allowAllPermissionSet());

        startClient(CLNT_ENDPOINT, allowAllPermissionSet());

        G.allGrids().get(0).cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void setupVerifier(Verifier verifier) {
        verifier
            .expect(SRV_FEATURE_CALL, 1)
            .expect(CLNT_FEATURE_CALL, 1)
            .expect(SRV_FEATURE_TRANSITION, 2)
            .expect(CLNT_FEATURE_TRANSITION, 2)
            .expect(SRV_ENDPOINT, 4)
            .expect(CLNT_ENDPOINT, 4);
    }

    /**
     *
     */
    @Test
    public void test() {
        runAndCheck(grid(SRV_INITIATOR));
        runAndCheck(grid(CLNT_INITIATOR));
    }

    /**
     * @param initiator Initiator node.
     */
    private void runAndCheck(IgniteEx initiator) {
        for (IgniteRunnable r : featureRuns()) {
            runAndCheck(secSubjectId(initiator),
                () -> compute(initiator, featureCalls()).broadcast((IgniteRunnable)
                    new CommonClosure(r)
                )
            );
        }
    }

    /**
     * @return Collection of consumers to call compute methods.
     */
    private List<IgniteRunnable> featureRuns() {
        return Arrays.asList(
            () -> compute(Ignition.localIgnite(), featureTransitions())
                .broadcast((IgniteRunnable)new CommonClosure(endpoints())),
            () -> compute(Ignition.localIgnite(), featureTransitions())
                .broadcastAsync((IgniteRunnable)new CommonClosure(endpoints()))
                .get(),
            () -> {
                for (UUID id : featureTransitions()) {
                    compute(Ignition.localIgnite(), id)
                        .call(new CommonClosure(endpoints()));
                }
            },
            () -> {
                for (UUID id : featureTransitions()) {
                    compute(Ignition.localIgnite(), id)
                        .callAsync(new CommonClosure(endpoints())).get();
                }
            },
            () -> {
                for (UUID id : featureTransitions()) {
                    compute(Ignition.localIgnite(), id)
                        .run(new CommonClosure(endpoints()));
                }
            },
            () -> {
                for (UUID id : featureTransitions()) {
                    compute(Ignition.localIgnite(), id)
                        .runAsync(new CommonClosure(endpoints())).get();
                }
            },
            () -> {
                for (UUID id : featureTransitions()) {
                    compute(Ignition.localIgnite(), id)
                        .apply(new CommonClosure(endpoints()), new Object());
                }
            },
            () -> {
                for (UUID id : featureTransitions()) {
                    compute(Ignition.localIgnite(), id)
                        .applyAsync(new CommonClosure(endpoints()), new Object()).get();
                }
            }
        );
    }

    /**
     * Common closure for tests.
     */
    static class CommonClosure implements IgniteRunnable, IgniteCallable<Object>,
        IgniteClosure<Object, Object> {

        /** Runnable. */
        private final IgniteRunnable runnable;

        /** Collection of endpoint node ids. */
        private final Collection<UUID> endpoints;

        /**
         * @param runnable Runnable.
         */
        public CommonClosure(IgniteRunnable runnable) {
            assert runnable != null;

            this.runnable = runnable;
            endpoints = Collections.emptyList();
        }

        /**
         * @param endpoints Collection of endpoint node ids.
         */
        private CommonClosure(Collection<UUID> endpoints) {
            assert !endpoints.isEmpty();

            runnable = null;
            this.endpoints = endpoints;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            register();

            Ignite ignite = Ignition.localIgnite();

            if (runnable != null)
                runnable.run();
            else {
                compute(ignite, endpoints)
                    .broadcast(() -> register());
            }
        }

        /** {@inheritDoc} */
        @Override public Object call() {
            run();

            return null;
        }

        /** {@inheritDoc} */
        @Override public Object apply(Object o) {
            run();

            return null;
        }
    }
}
