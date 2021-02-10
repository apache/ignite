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

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteRunnable;

import static java.util.Collections.singleton;
import static org.apache.ignite.Ignition.allGrids;

/**
 * Testing operation security context when the compute closure is executed on remote nodes.
 * <p>
 * The initiator node broadcasts a task to 'run' nodes that starts compute operation. That operation is executed on
 * 'check' nodes and broadcasts a task to 'endpoint' nodes. On every step, it is performed verification that operation
 * security context is the initiator context.
 */
public class DistributedAffinityClosureRemoteSecurityContextCheckTest
    extends DistributedClosureRemoteSecurityContextCheckTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridAllowAll(SRV_INITIATOR);

        startClientAllowAll(CLNT_INITIATOR);

        startGridAllowAll(SRV_RUN);

        startClientAllowAll(CLNT_RUN);

        startGridAllowAll(SRV_CHECK);

        startGridAllowAll(SRV_ENDPOINT);

        G.allGrids().get(0).cluster().state(ClusterState.ACTIVE);
    }

    /** {@inheritDoc} */
    @Override protected Collection<String> nodesToCheck() {
        return Collections.singletonList(SRV_CHECK);
    }

    /** {@inheritDoc} */
    @Override protected Collection<String> endpoints() {
        return Collections.singletonList(SRV_ENDPOINT);
    }

    /**
     * @return Stream of check cases.
     */
    @Override protected Stream<IgniteRunnable> operations() {
        return Stream.of(
            () -> {
                for (UUID id : nodesToCheckIds())
                    compute(id).affinityCall(singleton("test_cache"), getPartitionId(id), operationCheck());
            },
            () -> {
                for (UUID id : nodesToCheckIds())
                    compute(id).affinityRun(singleton("test_cache"), getPartitionId(id), operationCheck());
            }
        );
    }

    /**
     * @param nodeId Server node ID.
     * @return Partition ID for given server node.
     */
    private int getPartitionId(UUID nodeId) {
        IgniteCache<Object, Object> cache = allGrids()
            .stream()
            .filter(n -> nodeId.equals(n.cluster().localNode().id()))
            .findFirst()
            .get()
            .getOrCreateCache("test_cache");

        try {
            return primaryKey(cache);
        } catch (IgniteCheckedException e) {
            throw new RuntimeException(e);
        }
    }
}
