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

package org.apache.ignite.internal.processors.hadoop.impl;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.ignite.configuration.HadoopConfiguration;
import org.apache.ignite.hadoop.planner.HadoopTestRoundRobinMrPlanner;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.hadoop.Hadoop;
import org.apache.ignite.internal.processors.hadoop.HadoopJobId;
import org.apache.ignite.internal.processors.hadoop.HadoopJobStatus;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static org.apache.ignite.internal.processors.hadoop.impl.HadoopUtils.createJobInfo;
import static org.apache.ignite.internal.processors.hadoop.state.HadoopJobTrackerSelfTestState.COMBINE_EXEC_CNT;
import static org.apache.ignite.internal.processors.hadoop.state.HadoopJobTrackerSelfTestState.LATCH;
import static org.apache.ignite.internal.processors.hadoop.state.HadoopJobTrackerSelfTestState.MAP_EXEC_CNT;
import static org.apache.ignite.internal.processors.hadoop.state.HadoopJobTrackerSelfTestState.REDUCE_EXEC_CNT;

/**
 * Job tracker self test.
 */
public class HadoopJobTrackerSelfTest extends HadoopAbstractSelfTest {
    /** */
    private static final String PATH_OUTPUT = "/test-out";

    /** Test block count parameter name. */
    private static final int BLOCK_CNT = 10;

    /** {@inheritDoc} */
    @Override protected boolean igfsEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted0() throws Exception {
        startGrids(gridCount());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        LATCH.put("mapAwaitLatch", new CountDownLatch(1));
        LATCH.put("reduceAwaitLatch", new CountDownLatch(1));
        LATCH.put("combineAwaitLatch", new CountDownLatch(1));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        MAP_EXEC_CNT.set(0);
        COMBINE_EXEC_CNT.set(0);
        REDUCE_EXEC_CNT.set(0);
    }

    /** {@inheritDoc} */
    @Override public HadoopConfiguration hadoopConfiguration(String igniteInstanceName) {
        HadoopConfiguration cfg = super.hadoopConfiguration(igniteInstanceName);

        cfg.setMapReducePlanner(new HadoopTestRoundRobinMrPlanner());

        // TODO: IGNITE-404: Uncomment when fixed.
        //cfg.setExternalExecution(false);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSimpleTaskSubmit() throws Exception {
        try {
            UUID globalId = UUID.randomUUID();

            Job job = Job.getInstance();
            setupFileSystems(job.getConfiguration());

            job.setMapperClass(TestMapper.class);
            job.setReducerClass(TestReducer.class);
            job.setInputFormatClass(InFormat.class);

            FileOutputFormat.setOutputPath(job, new Path(igfsScheme() + PATH_OUTPUT + "1"));

            HadoopJobId jobId = new HadoopJobId(globalId, 1);

            grid(0).hadoop().submit(jobId, createJobInfo(job.getConfiguration(), null));

            checkStatus(jobId, false);

            info("Releasing map latch.");

            LATCH.get("mapAwaitLatch").countDown();

            checkStatus(jobId, false);

            info("Releasing reduce latch.");

            LATCH.get("reduceAwaitLatch").countDown();

            checkStatus(jobId, true);

            assertEquals(10, MAP_EXEC_CNT.get());
            assertEquals(0, COMBINE_EXEC_CNT.get());
            assertEquals(1, REDUCE_EXEC_CNT.get());
        }
        finally {
            // Safety.
            LATCH.get("mapAwaitLatch").countDown();
            LATCH.get("combineAwaitLatch").countDown();
            LATCH.get("reduceAwaitLatch").countDown();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTaskWithCombinerPerMap() throws Exception {
        try {
            UUID globalId = UUID.randomUUID();

            Job job = Job.getInstance();
            setupFileSystems(job.getConfiguration());

            job.setMapperClass(TestMapper.class);
            job.setReducerClass(TestReducer.class);
            job.setCombinerClass(TestCombiner.class);
            job.setInputFormatClass(InFormat.class);

            FileOutputFormat.setOutputPath(job, new Path(igfsScheme() + PATH_OUTPUT + "2"));

            HadoopJobId jobId = new HadoopJobId(globalId, 1);

            grid(0).hadoop().submit(jobId, createJobInfo(job.getConfiguration(), null));

            checkStatus(jobId, false);

            info("Releasing map latch.");

            LATCH.get("mapAwaitLatch").countDown();

            checkStatus(jobId, false);

            // All maps are completed. We have a combiner, so no reducers should be executed
            // before combiner latch is released.

            U.sleep(50);

            assertEquals(0, REDUCE_EXEC_CNT.get());

            info("Releasing combiner latch.");

            LATCH.get("combineAwaitLatch").countDown();

            checkStatus(jobId, false);

            info("Releasing reduce latch.");

            LATCH.get("reduceAwaitLatch").countDown();

            checkStatus(jobId, true);

            assertEquals(10, MAP_EXEC_CNT.get());
            assertEquals(10, COMBINE_EXEC_CNT.get());
            assertEquals(1, REDUCE_EXEC_CNT.get());
        }
        finally {
            // Safety.
            LATCH.get("mapAwaitLatch").countDown();
            LATCH.get("combineAwaitLatch").countDown();
            LATCH.get("reduceAwaitLatch").countDown();
        }
    }

    /**
     * Checks job execution status.
     *
     * @param jobId Job ID.
     * @param complete Completion status.
     * @throws Exception If failed.
     */
    private void checkStatus(HadoopJobId jobId, boolean complete) throws Exception {
        for (int i = 0; i < gridCount(); i++) {
            IgniteKernal kernal = (IgniteKernal)grid(i);

            Hadoop hadoop = kernal.hadoop();

            HadoopJobStatus stat = hadoop.status(jobId);

            assert stat != null;

            IgniteInternalFuture<?> fut = hadoop.finishFuture(jobId);

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

                @Override public float getProgress() {
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

            LATCH.get("mapAwaitLatch").await();

            MAP_EXEC_CNT.incrementAndGet();

            System.out.println("Completed task: " + ctx.getTaskAttemptID().getTaskID().getId());
        }
    }

    /**
     * Test reducer.
     */
    private static class TestReducer extends Reducer {
        @Override public void run(Context ctx) throws IOException, InterruptedException {
            System.out.println("Running task: " + ctx.getTaskAttemptID().getTaskID().getId());

            LATCH.get("reduceAwaitLatch").await();

            REDUCE_EXEC_CNT.incrementAndGet();

            System.out.println("Completed task: " + ctx.getTaskAttemptID().getTaskID().getId());
        }
    }

    /**
     * Test combiner.
     */
    private static class TestCombiner extends Reducer {
        @Override public void run(Context ctx) throws IOException, InterruptedException {
            System.out.println("Running task: " + ctx.getTaskAttemptID().getTaskID().getId());

            LATCH.get("combineAwaitLatch").await();

            COMBINE_EXEC_CNT.incrementAndGet();

            System.out.println("Completed task: " + ctx.getTaskAttemptID().getTaskID().getId());
        }
    }
}
