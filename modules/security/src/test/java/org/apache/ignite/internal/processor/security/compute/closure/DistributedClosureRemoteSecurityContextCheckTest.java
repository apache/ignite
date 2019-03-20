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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.processor.security.AbstractRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteRunnable;
import org.junit.Test;

/**
 * Testing operation security context when the compute closure is executed on remote nodes.
 * <p>
 * The initiator node broadcasts a task to run nodes that starts compute operation. That operation is executed
 * on check nodes and broadcasts a task to endpoint nodes. On every step, it is performed verification that
 * operation security context is the initiator context.
 */
public class DistributedClosureRemoteSecurityContextCheckTest
    extends AbstractRemoteSecurityContextCheckTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(SRV_INITIATOR, allowAllPermissionSet());

        startClient(CLNT_INITIATOR, allowAllPermissionSet());

        startGrid(SRV_RUN, allowAllPermissionSet());

        startClient(CLNT_RUN, allowAllPermissionSet());

        startGrid(SRV_CHECK, allowAllPermissionSet());

        startClient(CLNT_CHECK, allowAllPermissionSet());

        startGrid(SRV_ENDPOINT, allowAllPermissionSet());

        startClient(CLNT_ENDPOINT, allowAllPermissionSet());

        G.allGrids().get(0).cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void setupVerifier(Verifier verifier) {
        verifier
            .expect(SRV_RUN, 1)
            .expect(CLNT_RUN, 1)
            .expect(SRV_CHECK, 2)
            .expect(CLNT_CHECK, 2)
            .expect(SRV_ENDPOINT, 4)
            .expect(CLNT_ENDPOINT, 4);
    }

    /**
     *
     */
    @Test
    public void test() {
        runAndCheck(grid(SRV_INITIATOR), checkCases());
        runAndCheck(grid(CLNT_INITIATOR), checkCases());
    }

    /**
     * @return Collection of check cases.
     */
    private List<IgniteRunnable> checkCases() {
        return Stream.<IgniteRunnable>of(
            () -> compute(Ignition.localIgnite(), nodesToCheck())
                .broadcast((IgniteRunnable)new CommonClosure(endpoints())),

            () -> compute(Ignition.localIgnite(), nodesToCheck())
                .broadcastAsync((IgniteRunnable)new CommonClosure(endpoints()))
                .get(),
            () -> {
                for (UUID id : nodesToCheck()) {
                    compute(Ignition.localIgnite(), id)
                        .call(new CommonClosure(endpoints()));
                }
            },
            () -> {
                for (UUID id : nodesToCheck()) {
                    compute(Ignition.localIgnite(), id)
                        .callAsync(new CommonClosure(endpoints())).get();
                }
            },
            () -> {
                for (UUID id : nodesToCheck()) {
                    compute(Ignition.localIgnite(), id)
                        .run(new CommonClosure(endpoints()));
                }
            },
            () -> {
                for (UUID id : nodesToCheck()) {
                    compute(Ignition.localIgnite(), id)
                        .runAsync(new CommonClosure(endpoints())).get();
                }
            },
            () -> {
                for (UUID id : nodesToCheck()) {
                    compute(Ignition.localIgnite(), id)
                        .apply(new CommonClosure(endpoints()), new Object());
                }
            },
            () -> {
                for (UUID id : nodesToCheck()) {
                    compute(Ignition.localIgnite(), id)
                        .applyAsync(new CommonClosure(endpoints()), new Object()).get();
                }
            }
        )
            .map(CommonClosure::new)
            .collect(Collectors.toCollection(ArrayList::new));
    }
}
