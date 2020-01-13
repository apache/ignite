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

package org.apache.ignite.internal.processors.security.compute;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * https://issues.apache.org/jira/browse/IGNITE-12529
 */
public class LocalIgniteInsideComputeTaskCancelTest extends AbstractSecurityTest {
    /** Cancel method was executed. */
    private static final AtomicBoolean CANCELED = new AtomicBoolean();

    /** Waiting timeout. */
    private static final int TIMEOUT = 20_000;

    /** */
    private static final CyclicBarrier BARRIER = new CyclicBarrier(2);

    /** */
    private static String locNodeName;

    /**
     * Initiator is server node. Remote is server node.
     */
    @Test
    public void test() throws Exception {
        try {
            IgniteEx srv = startGridAllowAll("srv_init");

            IgniteEx rmt = startGridAllowAll("srv_rmt");

            srv.cluster().state(ClusterState.ACTIVE);

            locNodeName = null;

            //Checks the case when rmt node leaves the cluster.
            checkCancel(srv, rmt, f -> IgnitionEx.stop(rmt.name(), true, false));

            assertEquals(rmt.name(), locNodeName);
        }
        finally {
            G.stopAll(true);

            cleanPersistenceDir();
        }
    }

    /** */
    private void checkCancel(IgniteEx initator, IgniteEx rmt, Consumer<IgniteFuture> consumer) throws Exception {
        BARRIER.reset();
        CANCELED.set(false);

        IgniteFuture fut = initator.compute(initator.cluster().forNodeIds(Collections.singleton(rmt.localNode().id())))
            .executeAsync(new TestComputeTask(), 0);

        BARRIER.await(TIMEOUT, TimeUnit.MILLISECONDS);

        consumer.accept(fut);

        GridTestUtils.waitForCondition(CANCELED::get, TIMEOUT);
    }

    /** */
    private IgniteCompute compute(Ignite ignite, Collection<UUID> ids) {
        return ignite.compute(ignite.cluster().forNodeIds(ids));
    }

    /**
     * Compute task for tests.
     */
    static class TestComputeTask implements ComputeTask<Integer, Integer> {
        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            Integer arg) throws IgniteException {
            return Collections.singletonMap(
                new ComputeJob() {
                    @Override public void cancel() {
                        locNodeName = Ignition.localIgnite().name();

                        CANCELED.set(true);
                    }

                    @Override public Object execute() {
                        try {
                            BARRIER.await(TIMEOUT, TimeUnit.MILLISECONDS);

                            GridTestUtils.waitForCondition(() -> false, TIMEOUT);
                        }
                        catch (Exception e) {
                            throw new RuntimeException(e);
                        }

                        return null;
                    }
                }, subgrid.stream().findFirst().orElseThrow(IllegalStateException::new)
            );
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
            if (res.getException() != null)
                throw res.getException();

            return ComputeJobResultPolicy.WAIT;
        }

        /** {@inheritDoc} */
        @Override public @Nullable Integer reduce(List<ComputeJobResult> results) {
            return null;
        }
    }
}
