/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.marshaller.optimized.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Tests for {@code GridProjection.withXXX(..)} methods.
 */
public class GridTaskExecutionContextSelfTest extends GridCommonAbstractTest {
    /** */
    private static final AtomicInteger CNT = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMarshaller(new GridOptimizedMarshaller(false));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        CNT.set(0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testWithName() throws Exception {
        Callable<String> f = new GridCallable<String>() {
            @GridTaskSessionResource
            private GridComputeTaskSession ses;

            @Override public String call() {
                return ses.getTaskName();
            }
        };

        Ignite g = grid(0);

        assert "name1".equals(g.compute().withName("name1").call(f));
        assert "name2".equals(g.compute().withName("name2").call(f));
        assert f.getClass().getName().equals(g.compute().call(f));

        assert "name1".equals(g.compute().withName("name1").execute(new TestTask(false), null));
        assert "name2".equals(g.compute().withName("name2").execute(new TestTask(false), null));
        assert TestTask.class.getName().equals(g.compute().execute(new TestTask(false), null));
    }

    /**
     * @throws Exception If failed.
     */
    public void testWithNoFailoverClosure() throws Exception {
        final Runnable r = new GridAbsClosureX() {
            @Override public void applyx() throws GridException {
                CNT.incrementAndGet();

                throw new GridComputeExecutionRejectedException("Expected error.");
            }
        };

        final Ignite g = grid(0);

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    g.compute().withNoFailover().run(r);

                    return null;
                }
            },
            GridComputeExecutionRejectedException.class,
            "Expected error."
        );

        assertEquals(1, CNT.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testWithNoFailoverTask() throws Exception {
        final Ignite g = grid(0);

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    g.compute().withNoFailover().execute(new TestTask(true), null);

                    return null;
                }
            },
            GridComputeExecutionRejectedException.class,
            "Expected error."
        );

        assertEquals(1, CNT.get());
    }

    /**
     * Test task that returns its name.
     */
    private static class TestTask extends GridComputeTaskSplitAdapter<Void, String> {
        /** */
        private final boolean fail;

        /**
         * @param fail Whether to fail.
         */
        private TestTask(boolean fail) {
            this.fail = fail;
        }

        /** {@inheritDoc} */
        @Override protected Collection<? extends GridComputeJob> split(int gridSize, Void arg) throws GridException {
            return F.asSet(new GridComputeJobAdapter() {
                @GridTaskSessionResource
                private GridComputeTaskSession ses;

                @Override public Object execute() throws GridException {
                    CNT.incrementAndGet();

                    if (fail)
                        throw new GridComputeExecutionRejectedException("Expected error.");

                    return ses.getTaskName();
                }
            });
        }

        /** {@inheritDoc} */
        @Override public String reduce(List<GridComputeJobResult> results) throws GridException {
            return F.first(results).getData();
        }
    }
}
