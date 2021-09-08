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
import org.apache.ignite.cluster.ClusterState;
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

        G.allGrids().get(0).cluster().state(ClusterState.ACTIVE);
    }

    /** */
    @Test
    public void test() {
        runAndCheck(operations());
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
        return Stream.of(
            () -> compute(localIgnite(), nodesToCheckIds()).broadcast((IgniteRunnable) operationCheck()),
            () -> compute(localIgnite(), nodesToCheckIds()).broadcastAsync((IgniteRunnable) operationCheck()).get(),
            () -> {
                for (UUID id : nodesToCheckIds())
                    compute(id).call(operationCheck());
            },
            () -> {
                for (UUID id : nodesToCheckIds())
                    compute(id).callAsync(operationCheck()).get();
            },
            () -> {
                for (UUID id : nodesToCheckIds())
                    compute(id).run(operationCheck());
            },
            () -> {
                for (UUID id : nodesToCheckIds())
                    compute(id).runAsync(operationCheck()).get();
            },
            () -> {
                for (UUID id : nodesToCheckIds())
                    compute(id).apply(operationCheck(), new Object());
            },
            () -> {
                for (UUID id : nodesToCheckIds())
                    compute(id).applyAsync(operationCheck(), new Object()).get();
            }
        );
    }
}
