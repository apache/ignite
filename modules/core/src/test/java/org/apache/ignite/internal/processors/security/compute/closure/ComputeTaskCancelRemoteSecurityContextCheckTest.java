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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.security.AbstractRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Testing operation security context when the compute task is canceled on remote node.
 * <p>
 * The initiator node send a task on a remote node asynchronously.
 * The method {@code cancel} is called on IgniteFuture or
 * the remote node leaves the cluster that is the reason for calling the 'cancel' method of a ComputeJob.
 * This method should be executed on the remote node with the initiator
 * node security context.
 */
public class ComputeTaskCancelRemoteSecurityContextCheckTest extends AbstractRemoteSecurityContextCheckTest {
    /** Cancel method was executed. */
    private static final AtomicBoolean CANCELED = new AtomicBoolean();

    /** Waiting timeout. */
    private static final int TIMEOUT = 20_000;

    /** */
    private static final CyclicBarrier BARRIER = new CyclicBarrier(2);

    /** {@inheritDoc} */
    @Override protected void setupVerifier(Verifier verifier) {
        // No-op.
    }

    /**
     * Initiator is server node. Remote is server node.
     */
    @Test
    public void testSrvTaskInitatorCancelOnSrvNode() throws Exception {
        prepareAndCheck(false, false);
    }

    /**
     * Initiator is server node. Remote is client node.
     */
    @Test
    public void testSrvTaskInitatorCancelOnClientNode() throws Exception {
        prepareAndCheck(false, true);
    }

    /**
     * Initiator is client node. Remote is server node.
     */
    @Test
    public void testClientTaskInitatorCancelOnSrvNode() throws Exception {
        prepareAndCheck(true, false);
    }

    /**
     * Initiator is client node. Remote is client node.
     */
    @Test
    public void testClientTaskInitatorCancelOnClientNode() throws Exception {
        prepareAndCheck(true, true);
    }

    /**
     * @param isClientInitiator The initiator node is client.
     * @param isClientRmt The remote node is client.
     */
    private void prepareAndCheck(boolean isClientInitiator, boolean isClientRmt) throws Exception {
        try {
            IgniteEx srv = startGridAllowAll("srv_init");

            IgniteEx initator = isClientInitiator ? startClientAllowAll("clnt_init") : srv;

            IgniteEx rmt = isClientRmt ? startClientAllowAll("clnt_rmt") : startGridAllowAll("srv_rmt");

            srv.cluster().state(ClusterState.ACTIVE);

            //Checks the case when IgniteFuture#cancel is called.
            checkCancel(initator, rmt, IgniteFuture::cancel);
            //Checks the case when rmt node leaves the cluster.
            checkCancel(initator, rmt, f -> stopGrid(rmt.name(), true));
        }
        finally {
            G.stopAll(true);

            cleanPersistenceDir();
        }
    }

    /** */
    private void checkCancel(IgniteEx initator, IgniteEx rmt, Consumer<IgniteFuture> consumer) throws Exception {
        VERIFIER
            .initiator(initator)
            .expect(rmt.name(), OPERATION_START, 1);

        BARRIER.reset();
        CANCELED.set(false);

        IgniteFuture fut = compute(initator, Collections.singleton(rmt.localNode().id()))
            .executeAsync(new TestComputeTask(), 0);

        BARRIER.await(TIMEOUT, TimeUnit.MILLISECONDS);

        consumer.accept(fut);

        GridTestUtils.waitForCondition(CANCELED::get, TIMEOUT);

        VERIFIER.checkResult();
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
                        VERIFIER.register(OPERATION_START);

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
