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
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.HadoopConfiguration;
import org.apache.ignite.hadoop.fs.v1.IgniteHadoopFileSystem;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.hadoop.Hadoop;
import org.apache.ignite.internal.processors.hadoop.HadoopJobId;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskCancelledException;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.processors.hadoop.state.HadoopTaskExecutionSelfTestValues.CANCELLED_TASKS;
import static org.apache.ignite.internal.processors.hadoop.state.HadoopTaskExecutionSelfTestValues.EXECUTED_TASKS;
import static org.apache.ignite.internal.processors.hadoop.state.HadoopTaskExecutionSelfTestValues.FAIL_MAPPER_ID;
import static org.apache.ignite.internal.processors.hadoop.state.HadoopTaskExecutionSelfTestValues.SPLITS_COUNT;
import static org.apache.ignite.internal.processors.hadoop.state.HadoopTaskExecutionSelfTestValues.TASK_WORK_DIRS;
import static org.apache.ignite.internal.processors.hadoop.state.HadoopTaskExecutionSelfTestValues.TOTAL_LINE_CNT;
import static org.apache.ignite.internal.processors.hadoop.impl.HadoopUtils.createJobInfo;

/**
 * Tests map-reduce task execution basics.
 */
public class HadoopTaskExecutionSelfTest extends HadoopAbstractSelfTest {
    /** Test param. */
    private static final String MAP_WRITE = "test.map.write";

    /** {@inheritDoc} */
    @Override public FileSystemConfiguration igfsConfiguration() throws Exception {
        FileSystemConfiguration cfg = super.igfsConfiguration();

        cfg.setFragmentizerEnabled(false);

        return cfg;
    }

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
        grid(0).fileSystem(IGFS_NAME).clear();
    }

    /** {@inheritDoc} */
    @Override public HadoopConfiguration hadoopConfiguration(String igniteInstanceName) {
        HadoopConfiguration cfg = super.hadoopConfiguration(igniteInstanceName);

        cfg.setMaxParallelTasks(5);

        // TODO: IGNITE-404: Uncomment when fixed.
        //cfg.setExternalExecution(false);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMapRun() throws Exception {
        int lineCnt = 10000;
        String fileName = "/testFile";

        prepareFile(fileName, lineCnt);

        TOTAL_LINE_CNT.set(0);
        TASK_WORK_DIRS.clear();

        Configuration cfg = new Configuration();

        cfg.setStrings("fs.igfs.impl", IgniteHadoopFileSystem.class.getName());

        Job job = Job.getInstance(cfg);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(TestMapper.class);

        job.setNumReduceTasks(0);

        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("igfs://" + IGFS_NAME + "@/"));
        FileOutputFormat.setOutputPath(job, new Path("igfs://" + IGFS_NAME + "@/output/"));

        job.setJarByClass(getClass());

        IgniteInternalFuture<?> fut = grid(0).hadoop().submit(new HadoopJobId(UUID.randomUUID(), 1),
                createJobInfo(job.getConfiguration(), null));

        fut.get();

        assertEquals(lineCnt, TOTAL_LINE_CNT.get());

        assertEquals(32, TASK_WORK_DIRS.size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMapCombineRun() throws Exception {
        int lineCnt = 10001;
        String fileName = "/testFile";

        prepareFile(fileName, lineCnt);

        TOTAL_LINE_CNT.set(0);
        TASK_WORK_DIRS.clear();

        Configuration cfg = new Configuration();

        cfg.setStrings("fs.igfs.impl", IgniteHadoopFileSystem.class.getName());
        cfg.setBoolean(MAP_WRITE, true);

        Job job = Job.getInstance(cfg);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(TestMapper.class);
        job.setCombinerClass(TestCombiner.class);
        job.setReducerClass(TestReducer.class);

        job.setNumReduceTasks(2);

        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("igfs://" + IGFS_NAME + "@/"));
        FileOutputFormat.setOutputPath(job, new Path("igfs://" + IGFS_NAME + "@/output/"));

        job.setJarByClass(getClass());

        HadoopJobId jobId = new HadoopJobId(UUID.randomUUID(), 2);

        IgniteInternalFuture<?> fut = grid(0).hadoop().submit(jobId, createJobInfo(job.getConfiguration(), null));

        fut.get();

        assertEquals(lineCnt, TOTAL_LINE_CNT.get());

        assertEquals(34, TASK_WORK_DIRS.size());

        for (int g = 0; g < gridCount(); g++)
            grid(g).hadoop().finishFuture(jobId).get();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMapperException() throws Exception {
        prepareFile("/testFile", 1000);

        Configuration cfg = new Configuration();

        cfg.setStrings("fs.igfs.impl", IgniteHadoopFileSystem.class.getName());

        Job job = Job.getInstance(cfg);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(FailMapper.class);

        job.setNumReduceTasks(0);

        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("igfs://" + IGFS_NAME + "@/"));
        FileOutputFormat.setOutputPath(job, new Path("igfs://" + IGFS_NAME + "@/output/"));

        job.setJarByClass(getClass());

        final IgniteInternalFuture<?> fut = grid(0).hadoop().submit(new HadoopJobId(UUID.randomUUID(), 3),
                createJobInfo(job.getConfiguration(), null));

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                fut.get();

                return null;
            }
        }, IgniteCheckedException.class, null);
    }

    /**
     * @param fileName File name.
     * @param lineCnt Line count.
     * @throws Exception If failed.
     */
    private void prepareFile(String fileName, int lineCnt) throws Exception {
        IgniteFileSystem igfs = grid(0).fileSystem(IGFS_NAME);

        try (OutputStream os = igfs.create(new IgfsPath(fileName), true)) {
            PrintWriter w = new PrintWriter(new OutputStreamWriter(os));

            for (int i = 0; i < lineCnt; i++)
                w.print("Hello, Hadoop map-reduce!\n");

            w.flush();
        }
    }

    /**
     * Prepare job with mappers to cancel.
     * @return Fully configured job.
     * @throws Exception If fails.
     */
    private Configuration prepareJobForCancelling() throws Exception {
        prepareFile("/testFile", 1500);

        EXECUTED_TASKS.set(0);
        CANCELLED_TASKS.set(0);
        FAIL_MAPPER_ID.set(0);
        SPLITS_COUNT.set(0);

        Configuration cfg = new Configuration();

        setupFileSystems(cfg);

        Job job = Job.getInstance(cfg);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(CancellingTestMapper.class);

        job.setNumReduceTasks(0);

        job.setInputFormatClass(InFormat.class);

        FileInputFormat.setInputPaths(job, new Path("igfs://" + IGFS_NAME + "@/"));
        FileOutputFormat.setOutputPath(job, new Path("igfs://" + IGFS_NAME + "@/output/"));

        job.setJarByClass(getClass());

        return job.getConfiguration();
    }

    /**
     * Test input format.
     */
    private static class InFormat extends TextInputFormat {
        @Override public List<InputSplit> getSplits(JobContext ctx) throws IOException {
            List<InputSplit> res = super.getSplits(ctx);

            SPLITS_COUNT.set(res.size());

            X.println("___ split of input: " + SPLITS_COUNT.get());

            return res;
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTaskCancelling() throws Exception {
        Configuration cfg = prepareJobForCancelling();

        HadoopJobId jobId = new HadoopJobId(UUID.randomUUID(), 1);

        final IgniteInternalFuture<?> fut = grid(0).hadoop().submit(jobId, createJobInfo(cfg, null));

        if (!GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return SPLITS_COUNT.get() > 0;
            }
        }, 20000)) {
            U.dumpThreads(log);

            assertTrue(false);
        }

        if (!GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return EXECUTED_TASKS.get() == SPLITS_COUNT.get();
            }
        }, 20000)) {
            U.dumpThreads(log);

            assertTrue(false);
        }

        // Fail mapper with id "1", cancels others
        FAIL_MAPPER_ID.set(1);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                fut.get();

                return null;
            }
        }, IgniteCheckedException.class, null);

        assertEquals(EXECUTED_TASKS.get(), CANCELLED_TASKS.get() + 1);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJobKill() throws Exception {
        Configuration cfg = prepareJobForCancelling();

        Hadoop hadoop = grid(0).hadoop();

        HadoopJobId jobId = new HadoopJobId(UUID.randomUUID(), 1);

        //Kill unknown job.
        boolean killRes = hadoop.kill(jobId);

        assertFalse(killRes);

        final IgniteInternalFuture<?> fut = hadoop.submit(jobId, createJobInfo(cfg, null));

        if (!GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return SPLITS_COUNT.get() > 0;
            }
        }, 20000)) {
            U.dumpThreads(log);

            assertTrue(false);
        }

        if (!GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                X.println("___ executed tasks: " + EXECUTED_TASKS.get());

                return EXECUTED_TASKS.get() == SPLITS_COUNT.get();
            }
        }, 20000)) {
            U.dumpThreads(log);

            fail();
        }

        //Kill really ran job.
        killRes = hadoop.kill(jobId);

        assertTrue(killRes);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                fut.get();

                return null;
            }
        }, IgniteCheckedException.class, null);

        assertEquals(EXECUTED_TASKS.get(), CANCELLED_TASKS.get());

        //Kill the same job again.
        killRes = hadoop.kill(jobId);

        assertFalse(killRes);
    }

    /**
     *
     */
    private static class CancellingTestMapper extends Mapper<Object, Text, Text, IntWritable> {
        private int mapperId;

        /** {@inheritDoc} */
        @Override protected void setup(Context ctx) throws IOException, InterruptedException {
            mapperId = EXECUTED_TASKS.incrementAndGet();
        }

        /** {@inheritDoc} */
        @Override public void run(Context ctx) throws IOException, InterruptedException {
            try {
                super.run(ctx);
            }
            catch (HadoopTaskCancelledException e) {
                CANCELLED_TASKS.incrementAndGet();

                throw e;
            }
        }

        /** {@inheritDoc} */
        @Override protected void map(Object key, Text val, Context ctx) throws IOException, InterruptedException {
            if (mapperId == FAIL_MAPPER_ID.get())
                throw new IOException();

            Thread.sleep(1000);
        }
    }

    /**
     * Test failing mapper.
     */
    private static class FailMapper extends Mapper<Object, Text, Text, IntWritable> {
        /** {@inheritDoc} */
        @Override protected void map(Object key, Text val, Context ctx) throws IOException, InterruptedException {
            throw new IOException("Expected");
        }
    }

    /**
     * Mapper calculates number of lines.
     */
    private static class TestMapper extends Mapper<Object, Text, Text, IntWritable> {
        /** Writable integer constant of '1'. */
        private static final IntWritable ONE = new IntWritable(1);

        /** Line count constant. */
        public static final Text LINE_COUNT = new Text("lineCount");

        /** {@inheritDoc} */
        @Override protected void setup(Context ctx) throws IOException, InterruptedException {
            X.println("___ Mapper: " + ctx.getTaskAttemptID());

            String taskId = ctx.getTaskAttemptID().toString();

            LocalFileSystem locFs = FileSystem.getLocal(ctx.getConfiguration());

            String workDir = locFs.getWorkingDirectory().toString();

            assertNull(TASK_WORK_DIRS.put(workDir, taskId));
        }

        /** {@inheritDoc} */
        @Override protected void map(Object key, Text val, Context ctx) throws IOException, InterruptedException {
            if (ctx.getConfiguration().getBoolean(MAP_WRITE, false))
                ctx.write(LINE_COUNT, ONE);
            else
                TOTAL_LINE_CNT.incrementAndGet();
        }
    }

    /**
     * Combiner calculates number of lines.
     */
    private static class TestCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        /** */
        IntWritable sum = new IntWritable();

        /** {@inheritDoc} */
        @Override protected void setup(Context ctx) throws IOException, InterruptedException {
            X.println("___ Combiner: ");
        }

        /** {@inheritDoc} */
        @Override protected void reduce(Text key, Iterable<IntWritable> values, Context ctx) throws IOException,
            InterruptedException {
            int lineCnt = 0;

            for (IntWritable value : values)
                lineCnt += value.get();

            sum.set(lineCnt);

            X.println("___ combo: " + lineCnt);

            ctx.write(key, sum);
        }
    }

    /**
     * Combiner calculates number of lines.
     */
    private static class TestReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        /** */
        IntWritable sum = new IntWritable();

        /** {@inheritDoc} */
        @Override protected void setup(Context ctx) throws IOException, InterruptedException {
            X.println("___ Reducer: " + ctx.getTaskAttemptID());

            String taskId = ctx.getTaskAttemptID().toString();
            String workDir = FileSystem.getLocal(ctx.getConfiguration()).getWorkingDirectory().toString();

            assertNull(TASK_WORK_DIRS.put(workDir, taskId));
        }

        /** {@inheritDoc} */
        @Override protected void reduce(Text key, Iterable<IntWritable> values, Context ctx) throws IOException,
            InterruptedException {
            int lineCnt = 0;

            for (IntWritable value : values) {
                lineCnt += value.get();

                X.println("___ rdcr: " + value.get());
            }

            sum.set(lineCnt);

            ctx.write(key, sum);

            X.println("___ RDCR SUM: " + lineCnt);

            TOTAL_LINE_CNT.addAndGet(lineCnt);
        }
    }
}
