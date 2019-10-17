package org.apache.ignite.internal.processors.security.compute.closure;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

public class ComputeJobCancelReproducerTest extends AbstractSecurityTest {
    /** Reentrant lock. */
    private static final ReentrantLock RNT_LOCK = new ReentrantLock();

    /** Reentrant lock timeout. */
    private static final int RNT_LOCK_TIMEOUT = 20_000;

    /** */
    private static AtomicReference<UUID> executeSubjectId = new AtomicReference<>(null);

    /** */
    private static AtomicReference<UUID> cancelSubjectId = new AtomicReference<>(null);

    @Test
    public void test() throws Exception {
        IgniteEx srv = startGrid("srv", ALLOW_ALL, false);

        IgniteEx clnt = startGrid("clnt", ALLOW_ALL, true);

        srv.cluster().active(true);

        RNT_LOCK.lock();

        try {
            final UUID subjectId = clnt.localNode().id();

            clnt.compute(clnt.cluster().forNode(srv.localNode()))
                .executeAsync(new TestCancelComputeTask(), 0);

            IgnitionEx.stop(clnt.name(), true, false);

            assertEquals(subjectId, executeSubjectId.get());
            assertEquals(subjectId, cancelSubjectId.get());
        }
        finally {
            RNT_LOCK.unlock();
        }
    }

    /** */
    static class TestCancelComputeTask implements ComputeTask<Object, Object> {
        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            Object arg) throws IgniteException {
            return Collections.singletonMap(
                new ComputeJob() {
                    @Override public void cancel() {
                        SecurityContext secCtx = IgnitionEx.localIgnite().context().security().securityContext();

                        cancelSubjectId.set(secCtx.subject().id());
                    }

                    @Override public Object execute() {
                        SecurityContext secCtx = IgnitionEx.localIgnite().context().security().securityContext();

                        executeSubjectId.set(secCtx.subject().id());

                        waitForCancel();

                        return null;
                    }
                }, subgrid.stream().findFirst().orElseThrow(IllegalStateException::new)
            );
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res,
            List<ComputeJobResult> rcvd) throws IgniteException {
            if (res.getException() != null)
                throw res.getException();

            return ComputeJobResultPolicy.REDUCE;
        }

        /** {@inheritDoc} */
        @Override public @Nullable Object reduce(List<ComputeJobResult> results) throws IgniteException {
            return null;
        }
    }

    /** Waits for InterruptedException on RNT_LOCK. */
    private static void waitForCancel() {
        boolean isLocked = false;

        try {
            isLocked = RNT_LOCK.tryLock(RNT_LOCK_TIMEOUT, TimeUnit.MILLISECONDS);

            if (!isLocked)
                throw new IgniteException("tryLock should succeed or interrupted");
        }
        catch (InterruptedException e) {
            System.out.println("MY_DEBUG waitForCancel InterruptedException");
            e.printStackTrace();
            // This is expected.
        }
        finally {
            if (isLocked)
                RNT_LOCK.unlock();
        }
    }

}
