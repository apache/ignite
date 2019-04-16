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

package org.apache.ignite.internal.processors.security.compute.closure;

import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.internal.processors.security.AbstractRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteRunnable;
import org.junit.Test;

import static org.apache.ignite.Ignition.localIgnite;

/**
 * Testing operation security context when the compute closure is executed on remote nodes.
 * <p>
 * The initiator node broadcasts a task to 'run' nodes that starts compute operation. That operation is executed on
 * 'check' nodes and broadcasts a task to 'endpoint' nodes. On every step, it is performed verification that operation
 * security context is the initiator context.
 */
public class DistributedClosureRemoteSecurityContextCheckTest extends AbstractRemoteSecurityContextCheckTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridAllowAll(SRV_INITIATOR);

        startClientAllowAll(CLNT_INITIATOR);

        startGridAllowAll(SRV_RUN);

        startClientAllowAll(CLNT_RUN);

        startGridAllowAll(SRV_CHECK);

        startClientAllowAll(CLNT_CHECK);

        startGridAllowAll(SRV_ENDPOINT);

        startClientAllowAll(CLNT_ENDPOINT);

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

    /** */
    @Test
    public void test() {
        runAndCheck(grid(SRV_INITIATOR), operations());
        runAndCheck(grid(CLNT_INITIATOR), operations());
    }

    /** */
    private IgniteCompute compute(UUID id) {
        Ignite loc = localIgnite();

        return loc.compute(loc.cluster().forNodeId(id));
    }

    /**
     * @return Stream of check cases.
     */
    private Stream<IgniteRunnable> operations() {
        return Stream.<IgniteRunnable>of(
            () -> compute(localIgnite(), nodesToCheck()).broadcast((IgniteRunnable) createRunner()),
            () -> compute(localIgnite(), nodesToCheck()).broadcastAsync((IgniteRunnable) createRunner()).get(),
            () -> {
                for (UUID id : nodesToCheck())
                    compute(id).call(createRunner());
            },
            () -> {
                for (UUID id : nodesToCheck())
                    compute(id).callAsync(createRunner()).get();
            },
            () -> {
                for (UUID id : nodesToCheck())
                    compute(id).run(createRunner());
            },
            () -> {
                for (UUID id : nodesToCheck())
                    compute(id).runAsync(createRunner()).get();
            },
            () -> {
                for (UUID id : nodesToCheck())
                    compute(id).apply(createRunner(), new Object());
            },
            () -> {
                for (UUID id : nodesToCheck())
                    compute(id).applyAsync(createRunner(), new Object()).get();
            }
        ).map(RegisterExecAndForward::new);
    }
}
