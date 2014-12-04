/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.gridgain.client.*;
import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.events.GridEventType.*;

/**
 * Tests for security subject ID in task events.
 */
public class GridTaskEventSubjectIdSelfTest extends GridCommonAbstractTest {
    /** */
    private static final Collection<GridTaskEvent> evts = new ArrayList<>();

    /** */
    private static CountDownLatch latch;

    /** */
    private static UUID nodeId;

    /** */
    private static GridClient client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setRestEnabled(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        Ignite g = startGrid();

        g.events().localListen(new IgnitePredicate<GridEvent>() {
            @Override public boolean apply(GridEvent evt) {
                assert evt instanceof GridTaskEvent;

                evts.add((GridTaskEvent)evt);

                latch.countDown();

                return true;
            }
        }, EVTS_TASK_EXECUTION);

        nodeId = g.cluster().localNode().id();

        GridClientConfiguration cfg = new GridClientConfiguration();

        cfg.setServers(Collections.singleton("127.0.0.1:11211"));

        client = GridClientFactory.start(cfg);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        GridClientFactory.stop(client.id());

        stopGrid();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        evts.clear();
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleTask() throws Exception {
        latch = new CountDownLatch(3);

        grid().compute().execute(new SimpleTask(), null);

        assert latch.await(1000, MILLISECONDS);

        assertEquals(3, evts.size());

        Iterator<GridTaskEvent> it = evts.iterator();

        assert it.hasNext();

        GridTaskEvent evt = it.next();

        assert evt != null;

        assertEquals(EVT_TASK_STARTED, evt.type());
        assertEquals(nodeId, evt.subjectId());

        assert it.hasNext();

        evt = it.next();

        assert evt != null;

        assertEquals(EVT_TASK_REDUCED, evt.type());
        assertEquals(nodeId, evt.subjectId());

        assert it.hasNext();

        evt = it.next();

        assert evt != null;

        assertEquals(EVT_TASK_FINISHED, evt.type());
        assertEquals(nodeId, evt.subjectId());

        assert !it.hasNext();
    }

    /**
     * @throws Exception If failed.
     */
    public void testFailedTask() throws Exception {
        latch = new CountDownLatch(2);

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    grid().compute().execute(new FailedTask(), null);

                    return null;
                }
            },
            GridException.class,
            null
        );

        assert latch.await(1000, MILLISECONDS);

        assertEquals(2, evts.size());

        Iterator<GridTaskEvent> it = evts.iterator();

        assert it.hasNext();

        GridTaskEvent evt = it.next();

        assert evt != null;

        assertEquals(EVT_TASK_STARTED, evt.type());
        assertEquals(nodeId, evt.subjectId());

        assert it.hasNext();

        evt = it.next();

        assert evt != null;

        assertEquals(EVT_TASK_FAILED, evt.type());
        assertEquals(nodeId, evt.subjectId());

        assert !it.hasNext();
    }

    /**
     * @throws Exception If failed.
     */
    public void testTimedOutTask() throws Exception {
        latch = new CountDownLatch(2);

        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    grid().compute().withTimeout(100).execute(new TimedOutTask(), null);

                    return null;
                }
            },
            GridComputeTaskTimeoutException.class,
            null
        );

        assert latch.await(1000, MILLISECONDS);

        assertEquals(3, evts.size());

        Iterator<GridTaskEvent> it = evts.iterator();

        assert it.hasNext();

        GridTaskEvent evt = it.next();

        assert evt != null;

        assertEquals(EVT_TASK_STARTED, evt.type());
        assertEquals(nodeId, evt.subjectId());

        assert it.hasNext();

        evt = it.next();

        assert evt != null;

        assertEquals(EVT_TASK_TIMEDOUT, evt.type());
        assertEquals(nodeId, evt.subjectId());

        assert it.hasNext();

        evt = it.next();

        assert evt != null;

        assertEquals(EVT_TASK_FAILED, evt.type());
        assertEquals(nodeId, evt.subjectId());

        assert !it.hasNext();
    }

    /**
     * @throws Exception If failed.
     */
    public void testClosure() throws Exception {
        latch = new CountDownLatch(3);

        grid().compute().run(new IgniteRunnable() {
            @Override public void run() {
                // No-op.
            }
        });

        assert latch.await(1000, MILLISECONDS);

        assertEquals(3, evts.size());

        Iterator<GridTaskEvent> it = evts.iterator();

        assert it.hasNext();

        GridTaskEvent evt = it.next();

        assert evt != null;

        assertEquals(EVT_TASK_STARTED, evt.type());
        assertEquals(nodeId, evt.subjectId());

        assert it.hasNext();

        evt = it.next();

        assert evt != null;

        assertEquals(EVT_TASK_REDUCED, evt.type());
        assertEquals(nodeId, evt.subjectId());

        assert it.hasNext();

        evt = it.next();

        assert evt != null;

        assertEquals(EVT_TASK_FINISHED, evt.type());
        assertEquals(nodeId, evt.subjectId());

        assert !it.hasNext();
    }

    /**
     * @throws Exception If failed.
     */
    public void testClient() throws Exception {
        latch = new CountDownLatch(3);

        client.compute().execute(SimpleTask.class.getName(), null);

        assert latch.await(1000, MILLISECONDS);

        assertEquals(3, evts.size());

        Iterator<GridTaskEvent> it = evts.iterator();

        assert it.hasNext();

        GridTaskEvent evt = it.next();

        assert evt != null;

        assertEquals(EVT_TASK_STARTED, evt.type());
        assertEquals(client.id(), evt.subjectId());

        assert it.hasNext();

        evt = it.next();

        assert evt != null;

        assertEquals(EVT_TASK_REDUCED, evt.type());
        assertEquals(client.id(), evt.subjectId());

        assert it.hasNext();

        evt = it.next();

        assert evt != null;

        assertEquals(EVT_TASK_FINISHED, evt.type());
        assertEquals(client.id(), evt.subjectId());

        assert !it.hasNext();
    }

    /** */
    private static class SimpleTask extends GridComputeTaskSplitAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends GridComputeJob> split(int gridSize, Object arg) throws GridException {
            return Collections.singleton(new GridComputeJobAdapter() {
                @Nullable @Override public Object execute() {
                    return null;
                }
            });
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
            return null;
        }
    }

    /** */
    private static class FailedTask extends GridComputeTaskSplitAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends GridComputeJob> split(int gridSize, Object arg) throws GridException {
            return Collections.singleton(new GridComputeJobAdapter() {
                @Nullable @Override public Object execute() {
                    return null;
                }
            });
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
            throw new GridException("Task failed.");
        }
    }

    /** */
    private static class TimedOutTask extends GridComputeTaskSplitAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends GridComputeJob> split(int gridSize, Object arg) throws GridException {
            return Collections.singleton(new GridComputeJobAdapter() {
                @Nullable @Override public Object execute() {
                    try {
                        Thread.sleep(10000);
                    }
                    catch (InterruptedException ignored) {
                        // No-op.
                    }

                    return null;
                }
            });
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
            return null;
        }
    }
}
