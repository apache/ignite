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
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.security.AbstractRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Testing operation security context when the compute task is canceled on remote node.
 * <p>
 * The initiator node send a task on a remote node asynchronously. The remote node leaves the cluster that is the reason
 * for calling the 'cancel' method of a ComputeJob. This method should be executed on the remote node with the initiator
 * node security context.
 */
public class ComputeTaskCancelRemoteSecurityContextCheckTest extends AbstractRemoteSecurityContextCheckTest {
    /** Reentrant lock. */
    private static final ReentrantLock RNT_LOCK = new ReentrantLock();

    /** Reentrant lock timeout. */
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
        checkCancel(false, false);
    }

    /**
     * Initiator is server node. Remote is client node.
     */
    @Test
    public void testSrvTaskInitatorCancelOnClientNode() throws Exception {
        checkCancel(false, true);
    }

    /**
     * Initiator is client node. Remote is server node.
     */
    @Test
    public void testClientTaskInitatorCancelOnSrvNode() throws Exception {
        checkCancel(true, false);
    }

    /**
     * Initiator is client node. Remote is client node.
     */
    @Test
    public void testClientTaskInitatorCancelOnClientNode() throws Exception {
        checkCancel(true, true);
    }

    /**
     * @param isClientInitiator The initiator node is client.
     * @param isClientRmt The remote node is client.
     */
    private void checkCancel(boolean isClientInitiator, boolean isClientRmt) throws Exception {
        try {
            IgniteEx srv = startGridAllowAll("srv_init");

            IgniteEx initator = isClientInitiator ? startClientAllowAll("clnt_init") : srv;

            IgniteEx rmt = isClientRmt ? startClientAllowAll("clnt_rmt") : startGridAllowAll("srv_rmt");

            srv.cluster().active(true);

            VERIFIER
                .clear()
                .initiator(initator)
                .expect(rmt.name(), 1);

            BARRIER.reset();
            RNT_LOCK.lock();

            try {
                compute(initator, Collections.singleton(rmt.localNode().id()))
                    .executeAsync(new TestComputeTask(), 0);

                BARRIER.await(TIMEOUT, TimeUnit.MILLISECONDS);

                IgnitionEx.stop(rmt.name(), true, false);
            }
            finally {
                RNT_LOCK.unlock();
            }

            VERIFIER.checkResult();
        }
        finally {
            G.stopAll(true);

            cleanPersistenceDir();
        }
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
                    @IgniteInstanceResource
                    private Ignite loc;

                    @Override public void cancel() {
                        VERIFIER.register((IgniteEx)loc);
                    }

                    @Override public Object execute() {
                        try {
                            BARRIER.await(TIMEOUT, TimeUnit.MILLISECONDS);
                        }
                        catch (Exception e) {
                            throw new RuntimeException(e);
                        }

                        waitForCancel();

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

    /** Waits for InterruptedException on RNT_LOCK. */
    private static void waitForCancel() {
        boolean isLocked = false;

        try {
            isLocked = RNT_LOCK.tryLock(TIMEOUT, TimeUnit.MILLISECONDS);

            if (!isLocked)
                throw new IgniteException("tryLock should succeed or interrupted");
        }
        catch (InterruptedException e) {
            // This is expected.
        }
        finally {
            if (isLocked)
                RNT_LOCK.unlock();
        }
    }
}
