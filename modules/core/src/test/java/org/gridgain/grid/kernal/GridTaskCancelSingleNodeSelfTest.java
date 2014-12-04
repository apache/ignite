/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.events.GridEventType.*;

/**
 * Test for task cancellation issue.
 * <p/>
 * http://www.gridgainsystems.com/jiveforums/thread.jspa?messageID=8034
 */
public class GridTaskCancelSingleNodeSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopGrid();
    }

    /**
     * @throws Exception If failed.
     */
    public void testImmediateCancellation() throws Exception {
        checkCancellation(0L);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCancellation() throws Exception {
        checkCancellation(2000L);
    }

    /**
     * @param timeoutBeforeCancel Timeout.
     * @throws Exception If failed.
     */
    @SuppressWarnings("ErrorNotRethrown")
    private void checkCancellation(long timeoutBeforeCancel) throws Exception {
        final AtomicInteger finished = new AtomicInteger();
        final AtomicInteger cancelled = new AtomicInteger();
        final AtomicInteger rejected = new AtomicInteger();

        grid().events().localListen(new IgnitePredicate<GridEvent>() {
            @Override public boolean apply(GridEvent evt) {
                info("Received event: " + evt);

                switch (evt.type()) {
                    case EVT_JOB_FINISHED:
                        finished.incrementAndGet();

                        break;

                    case EVT_JOB_CANCELLED:
                        cancelled.incrementAndGet();

                        break;

                    case EVT_JOB_REJECTED:
                        rejected.incrementAndGet();

                        break;

                    default:
                        assert false : "Unexpected event: " + evt;
                }

                return true;
            }
        }, EVT_JOB_FINISHED, EVT_JOB_CANCELLED, EVT_JOB_REJECTED);

        IgniteCompute comp = grid().compute().enableAsync();

        comp.execute(TestTask.class, null);

        GridComputeTaskFuture<?> fut = comp.future();

        if (timeoutBeforeCancel > 0L)
            Thread.sleep(timeoutBeforeCancel);

        assert fut.cancel();

        for (int i = 0; i < 3; i++) {
            try {
            if (timeoutBeforeCancel == 0L)
                assert (finished.get() == 0 && cancelled.get() == 0 && rejected.get() == 0) :
                    "Failed on iteration [i=" + i + ", finished=" + finished.get() +
                    ", cancelled=" + cancelled.get() + ", rejected=" + rejected.get() + ']';
            else
                assert (finished.get() == 1 && cancelled.get() == 1 && rejected.get() == 0) :
                    "Failed on iteration [i=" + i + ", finished=" + finished.get() +
                        ", cancelled=" + cancelled.get() + ", rejected=" + rejected.get() + ']';
            }
            catch (AssertionError e) {
                info("Check failed: " + e.getMessage());

                if (timeoutBeforeCancel == 0L && i == 2)
                    throw e;
            }

            if (i < 2)
                U.sleep(500);
        }

        try {
            fut.get();

            assert false;
        }
        catch (IgniteFutureCancelledException e) {
            info("Caught expected exception: " + e);
        }
    }

    /**
     *
     */
    @GridComputeTaskMapAsync
    private static class TestTask extends GridComputeTaskSplitAdapter<Void, Void> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Void arg) {
            return F.asSet(new GridComputeJobAdapter() {
                /** */
                @GridLoggerResource
                private GridLogger log;

                /** */
                @GridInstanceResource
                private Ignite g;

                /** {@inheritDoc} */
                @Override public Object execute() {
                    log.info("Executing job on node: " + g.cluster().localNode().id());

                    try {
                        Thread.sleep(5000);
                    }
                    catch (InterruptedException ignored) {
                        log.info("Job thread has been interrupted.");

                        Thread.currentThread().interrupt();
                    }

                    return null;
                }
            });
        }

        /** {@inheritDoc} */
        @Nullable @Override public Void reduce(List<GridComputeJobResult> results) throws GridException {
            return null;
        }
    }
}
