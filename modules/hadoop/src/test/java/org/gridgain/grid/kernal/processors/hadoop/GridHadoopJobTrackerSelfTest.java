/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.ignite.lang.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.kernal.processors.hadoop.GridHadoopUtils.*;

/**
 * Job tracker self test.
 */
public class GridHadoopJobTrackerSelfTest extends GridHadoopAbstractSelfTest {
    /** */
    private static final String PATH_OUTPUT = "/test-out";

    /** Test block count parameter name. */
    private static final int BLOCK_CNT = 10;

    /** */
    private static GridHadoopSharedMap m = GridHadoopSharedMap.map(GridHadoopJobTrackerSelfTest.class);

    /** Map task execution count. */
    private static final AtomicInteger mapExecCnt = m.put("mapExecCnt", new AtomicInteger());

    /** Reduce task execution count. */
    private static final AtomicInteger reduceExecCnt = m.put("reduceExecCnt", new AtomicInteger());

    /** Reduce task execution count. */
    private static final AtomicInteger combineExecCnt = m.put("combineExecCnt", new AtomicInteger());

    /** */
    private static final Map<String, CountDownLatch> latch = m.put("latch", new HashMap<String, CountDownLatch>());

    /** {@inheritDoc} */
    @Override protected boolean ggfsEnabled() {
        return true;
    }

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
        latch.put("mapAwaitLatch", new CountDownLatch(1));
        latch.put("reduceAwaitLatch", new CountDownLatch(1));
        latch.put("combineAwaitLatch", new CountDownLatch(1));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        mapExecCnt.set(0);
        combineExecCnt.set(0);
        reduceExecCnt.set(0);
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

            Job job = Job.getInstance();
            setupFileSystems(job.getConfiguration());

            job.setMapperClass(TestMapper.class);
            job.setReducerClass(TestReducer.class);
            job.setInputFormatClass(InFormat.class);

            FileOutputFormat.setOutputPath(job, new Path(ggfsScheme() + PATH_OUTPUT + "1"));

            GridHadoopJobId jobId = new GridHadoopJobId(globalId, 1);

            grid(0).hadoop().submit(jobId, createJobInfo(job.getConfiguration()));

            checkStatus(jobId, false);

            info("Releasing map latch.");

            latch.get("mapAwaitLatch").countDown();

            checkStatus(jobId, false);

            info("Releasing reduce latch.");

            latch.get("reduceAwaitLatch").countDown();

            checkStatus(jobId, true);

            assertEquals(10, mapExecCnt.get());
            assertEquals(0, combineExecCnt.get());
            assertEquals(1, reduceExecCnt.get());
        }
        finally {
            // Safety.
            latch.get("mapAwaitLatch").countDown();
            latch.get("combineAwaitLatch").countDown();
            latch.get("reduceAwaitLatch").countDown();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTaskWithCombinerPerMap() throws Exception {
        try {
            UUID globalId = UUID.randomUUID();

            Job job = Job.getInstance();
            setupFileSystems(job.getConfiguration());

            job.setMapperClass(TestMapper.class);
            job.setReducerClass(TestReducer.class);
            job.setCombinerClass(TestCombiner.class);
            job.setInputFormatClass(InFormat.class);

            FileOutputFormat.setOutputPath(job, new Path(ggfsScheme() + PATH_OUTPUT + "2"));

            GridHadoopJobId jobId = new GridHadoopJobId(globalId, 1);

            grid(0).hadoop().submit(jobId, createJobInfo(job.getConfiguration()));

            checkStatus(jobId, false);

            info("Releasing map latch.");

            latch.get("mapAwaitLatch").countDown();

            checkStatus(jobId, false);

            // All maps are completed. We have a combiner, so no reducers should be executed
            // before combiner latch is released.

            U.sleep(50);

            assertEquals(0, reduceExecCnt.get());

            info("Releasing combiner latch.");

            latch.get("combineAwaitLatch").countDown();

            checkStatus(jobId, false);

            info("Releasing reduce latch.");

            latch.get("reduceAwaitLatch").countDown();

            checkStatus(jobId, true);

            assertEquals(10, mapExecCnt.get());
            assertEquals(10, combineExecCnt.get());
            assertEquals(1, reduceExecCnt.get());
        }
        finally {
            // Safety.
            latch.get("mapAwaitLatch").countDown();
            latch.get("combineAwaitLatch").countDown();
            latch.get("reduceAwaitLatch").countDown();
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

            IgniteFuture<?> fut = hadoop.finishFuture(jobId);

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
     * Test input format
     */
    public static class InFormat extends InputFormat {

        @Override public List<InputSplit> getSplits(JobContext ctx) throws IOException, InterruptedException {
            List<InputSplit> res = new ArrayList<>(BLOCK_CNT);

            for (int i = 0; i < BLOCK_CNT; i++)
                try {
                    res.add(new FileSplit(new Path(new URI("someFile")), i, i + 1, new String[] {"localhost"}));
                }
                catch (URISyntaxException e) {
                    throw new IOException(e);
                }

            return res;
        }

        @Override public RecordReader createRecordReader(InputSplit split, TaskAttemptContext ctx) throws IOException, InterruptedException {
            return new RecordReader() {
                @Override public void initialize(InputSplit split, TaskAttemptContext ctx) {
                }

                @Override public boolean nextKeyValue() {
                    return false;
                }

                @Override public Object getCurrentKey() {
                    return null;
                }

                @Override public Object getCurrentValue() {
                    return null;
                }

                @Override public float getProgress() throws IOException, InterruptedException {
                    return 0;
                }

                @Override public void close() {

                }
            };
        }
    }

    /**
     * Test mapper.
     */
    private static class TestMapper extends Mapper {
        @Override public void run(Context ctx) throws IOException, InterruptedException {
            System.out.println("Running task: " + ctx.getTaskAttemptID().getTaskID().getId());

            latch.get("mapAwaitLatch").await();

            mapExecCnt.incrementAndGet();

            System.out.println("Completed task: " + ctx.getTaskAttemptID().getTaskID().getId());
        }
    }

    /**
     * Test reducer.
     */
    private static class TestReducer extends Reducer {
        @Override public void run(Context ctx) throws IOException, InterruptedException {
            System.out.println("Running task: " + ctx.getTaskAttemptID().getTaskID().getId());

            latch.get("reduceAwaitLatch").await();

            reduceExecCnt.incrementAndGet();

            System.out.println("Completed task: " + ctx.getTaskAttemptID().getTaskID().getId());
        }
    }

    /**
     * Test combiner.
     */
    private static class TestCombiner extends Reducer {
        @Override public void run(Context ctx) throws IOException, InterruptedException {
            System.out.println("Running task: " + ctx.getTaskAttemptID().getTaskID().getId());

            latch.get("combineAwaitLatch").await();

            combineExecCnt.incrementAndGet();

            System.out.println("Completed task: " + ctx.getTaskAttemptID().getTaskID().getId());
        }
    }
}
