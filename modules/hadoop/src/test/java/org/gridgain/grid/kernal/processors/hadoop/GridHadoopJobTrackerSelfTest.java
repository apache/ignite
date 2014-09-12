/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.gridgain.grid.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.hadoop.v2.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.kernal.processors.hadoop.GridHadoopUtils.*;

/**
 * Job tracker self test.
 */
public class GridHadoopJobTrackerSelfTest extends GridHadoopAbstractSelfTest {
    /** Test block count parameter name. */
    private static final String BLOCK_CNT = "test.block.count";

    /** Map task execution count. */
    private static final Map<UUID, AtomicInteger> mapExecCnt = new HashMap<>();

    /** Reduce task execution count. */
    private static final Map<UUID, AtomicInteger> reduceExecCnt = new HashMap<>();

    /** Reduce task execution count. */
    private static final Map<UUID, AtomicInteger> combineExecCnt = new HashMap<>();

    /** Map task await latch. */
    private static CountDownLatch mapAwaitLatch;

    /** Reduce task await latch. */
    private static CountDownLatch reduceAwaitLatch;

    /** Combine task await latch. */
    private static CountDownLatch combineAwaitLatch;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(gridCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        mapAwaitLatch = new CountDownLatch(1);
        reduceAwaitLatch = new CountDownLatch(1);
        combineAwaitLatch = new CountDownLatch(1);

        for (int i = 0; i < gridCount(); i++) {
            UUID nodeId = grid(i).localNode().id();

            mapExecCnt.put(nodeId, new AtomicInteger());
            combineExecCnt.put(nodeId, new AtomicInteger());
            reduceExecCnt.put(nodeId, new AtomicInteger());
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        mapExecCnt.clear();
        combineExecCnt.clear();
        reduceExecCnt.clear();
    }

    /** {@inheritDoc} */
    @Override public GridHadoopConfiguration hadoopConfiguration(String gridName) {
        GridHadoopConfiguration cfg = super.hadoopConfiguration(gridName);

        cfg.setMapReducePlanner(new GridHadoopTestRoundRobinMrPlanner());
        cfg.setExternalExecution(false);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleTaskSubmit() throws Exception {
        try {
            UUID globalId = UUID.randomUUID();

            Configuration cfg = new Configuration();

            int cnt = 10;

            cfg.setInt(BLOCK_CNT, cnt);

            GridHadoopJobId jobId = new GridHadoopJobId(globalId, 1);

            grid(0).hadoop().submit(jobId, new GridHadoopTestJobInfo(cfg));

            checkStatus(jobId, false);

            info("Releasing map latch.");

            mapAwaitLatch.countDown();

            checkStatus(jobId, false);

            info("Releasing reduce latch.");

            reduceAwaitLatch.countDown();

            checkStatus(jobId, true);

            int maps = 0;
            int reduces = 0;
            int combines = 0;

            for (int i = 0; i < gridCount(); i++) {
                Grid g = grid(i);

                UUID nodeId = g.localNode().id();

                maps += mapExecCnt.get(nodeId).get();
                combines += combineExecCnt.get(nodeId).get();
                reduces += reduceExecCnt.get(nodeId).get();
            }

            assertEquals(10, maps);
            assertEquals(0, combines);
            assertEquals(1, reduces);
        }
        finally {
            // Safety.
            mapAwaitLatch.countDown();
            combineAwaitLatch.countDown();
            reduceAwaitLatch.countDown();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTaskWithCombinerPerMap() throws Exception {
        try {
            UUID globalId = UUID.randomUUID();

            Configuration cfg = new Configuration();

            int cnt = 10;

            cfg.setInt(BLOCK_CNT, cnt);

            cfg.setClass(MRJobConfig.COMBINE_CLASS_ATTR, TestCombiner.class, Reducer.class);

            GridHadoopJobId jobId = new GridHadoopJobId(globalId, 1);

            grid(0).hadoop().submit(jobId, new GridHadoopTestJobInfo(cfg));

            checkStatus(jobId, false);

            info("Releasing map latch.");

            mapAwaitLatch.countDown();

            checkStatus(jobId, false);

            // All maps are completed. We have a combiner, so no reducers should be executed
            // before combiner latch is released.

            U.sleep(50);

            for (int i = 0; i < gridCount(); i++) {
                Grid g = grid(i);

                UUID nodeId = g.localNode().id();

                assertEquals(0, reduceExecCnt.get(nodeId).get());
            }

            info("Releasing combiner latch.");

            combineAwaitLatch.countDown();

            checkStatus(jobId, false);

            info("Releasing reduce latch.");

            reduceAwaitLatch.countDown();

            checkStatus(jobId, true);

            int maps = 0;
            int reduces = 0;
            int combines = 0;

            for (int i = 0; i < gridCount(); i++) {
                Grid g = grid(i);

                UUID nodeId = g.localNode().id();

                maps += mapExecCnt.get(nodeId).get();
                combines += combineExecCnt.get(nodeId).get();
                reduces += reduceExecCnt.get(nodeId).get();
            }

            assertEquals(10, maps);
            assertEquals(10, combines);
            assertEquals(1, reduces);
        }
        finally {
            // Safety.
            mapAwaitLatch.countDown();
            combineAwaitLatch.countDown();
            reduceAwaitLatch.countDown();
        }
    }

    /**
     * Checks job execution status.
     *
     * @param jobId Job ID.
     * @param complete Completion status.
     * @throws Exception If failed.
     */
    private void checkStatus(GridHadoopJobId jobId, boolean complete) throws Exception {
        for (int i = 0; i < gridCount(); i++) {
            GridKernal kernal = (GridKernal)grid(i);

            GridHadoop hadoop = kernal.hadoop();

            GridHadoopJobStatus stat = hadoop.status(jobId);

            assert stat != null;

            GridFuture<?> fut = hadoop.finishFuture(jobId);

            if (!complete)
                assertFalse(fut.isDone());
            else {
                info("Waiting for status future completion on node [idx=" + i + ", nodeId=" +
                    kernal.getLocalNodeId() + ']');

                fut.get();
            }
        }
    }

    /**
     * Test job info.
     */
    private static class GridHadoopTestJobInfo extends GridHadoopDefaultJobInfo {
        /** */
        private Configuration cfg;

        /**
         * @param cfg Config.
         */
        GridHadoopTestJobInfo(Configuration cfg) {
            this.cfg = cfg;
        }

        /**
         *
         */
        public GridHadoopTestJobInfo() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public GridHadoopJob createJob(GridHadoopJobId jobId, GridLogger log) throws GridException {
            return new HadoopTestJob(jobId, createJobInfo(cfg), log);
        }
    }

    /**
     * Test job.
     */
    private static class HadoopTestJob extends GridHadoopV2Job {
        /**
         * @param jobId Job ID.
         * @param jobInfoImpl Job info.
         */
        private HadoopTestJob(GridHadoopJobId jobId, GridHadoopDefaultJobInfo jobInfoImpl, GridLogger log) {
            super(jobId, jobInfoImpl, log);
        }

        /** {@inheritDoc} */
        @Override public Collection<GridHadoopInputSplit> input() throws GridException {
            int blocks = Integer.parseInt(jobInfo.property(BLOCK_CNT));

            Collection<GridHadoopInputSplit> res = new ArrayList<>(blocks);

            try {
                for (int i = 0; i < blocks; i++)
                    res.add(new GridHadoopFileBlock(new String[] {"localhost"}, new URI("someFile"), i, i + 1));

                return res;
            }
            catch (URISyntaxException e) {
                throw new GridException(e);
            }
        }
    }

    /**
     * Test task.
     */
    private static class HadoopTestTask extends GridHadoopTask {
        /**
         * @param taskInfo Task info.
         */
        private HadoopTestTask(GridHadoopTaskInfo taskInfo) {
            super(taskInfo);
        }

        /**
         *
         */
        public HadoopTestTask() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void run(GridHadoopTaskContext ctx) {
            try {
                UUID nodeId = info().nodeId();

                System.out.println("Running task: " + nodeId);

                switch (info().type()) {
                    case MAP: {
                        mapAwaitLatch.await();

                        mapExecCnt.get(nodeId).incrementAndGet();

                        break;
                    }

                    case REDUCE: {
                        reduceAwaitLatch.await();

                        reduceExecCnt.get(nodeId).incrementAndGet();

                        break;
                    }

                    case COMBINE: {
                        combineAwaitLatch.await();

                        combineExecCnt.get(nodeId).incrementAndGet();

                        break;
                    }

                    default: {
                        // No-op.
                        break;
                    }
                }

                System.out.println("Completed task on node: " + nodeId);
            }
            catch (InterruptedException ignore) {
                // Restore interrupted status.
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Test reducer. No invocations expected.
     */
    private static class TestCombiner extends Reducer<Void, Void, Void, Void> {

    }
}
