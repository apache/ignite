/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.resources.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Test task cancellation on grid stop.
 */
@SuppressWarnings({"ProhibitedExceptionDeclared"})
@GridCommonTest(group = "Kernal Self")
public class GridCancelOnGridStopSelfTest extends GridCommonAbstractTest {
    /** */
    private static CountDownLatch cnt;

    /** */
    private static boolean cancelCall;

    /** */
    public GridCancelOnGridStopSelfTest() {
        super(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCancelingJob() throws Exception {
        cancelCall = false;

        try (Ignite g = startGrid(1)) {
            cnt = new CountDownLatch(1);

            g.compute().enableAsync().execute(CancelledTask.class, null);

            cnt.await();
        }

        assert cancelCall;
    }

    /**
     * Cancelled task.
     */
    private static final class CancelledTask extends GridComputeTaskAdapter<String, Void> {
        /** */
        @GridLocalNodeIdResource
        private UUID locId;

        /** {@inheritDoc} */
        @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> subgrid, @Nullable String arg)
            throws GridException {
            for (GridNode node : subgrid) {
                if (node.id().equals(locId)) {
                    return Collections.singletonMap(new GridComputeJob() {
                        @Override public void cancel() {
                            cancelCall = true;
                        }

                        @Override public Serializable execute() throws GridException {
                            cnt.countDown();

                            try {
                                Thread.sleep(Long.MAX_VALUE);
                            }
                            catch (InterruptedException e) {
                                throw new GridException(e);
                            }

                            return null;
                        }
                    }, node);
                }
            }

            throw new GridException("Local node not found");
        }

        /** {@inheritDoc} */
        @Nullable @Override public Void reduce(List<GridComputeJobResult> results) throws GridException {
            return null;
        }
    }
}
