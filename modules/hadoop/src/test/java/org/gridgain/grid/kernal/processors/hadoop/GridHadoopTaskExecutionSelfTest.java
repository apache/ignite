/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.ggfs.hadoop.v1.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.kernal.processors.hadoop.GridHadoopUtils.*;

/**
 * Tests map-reduce task execution basics.
 */
public class GridHadoopTaskExecutionSelfTest extends GridHadoopAbstractSelfTest {
    /** Line count. */
    private static final AtomicInteger totalLineCnt = new AtomicInteger();

    /** Executed tasks. */
    private static final AtomicInteger executedTasks = new AtomicInteger();

    /** Cancelled tasks. */
    private static final AtomicInteger cancelledTasks = new AtomicInteger();

    /** Working directory of each task. */
    private static final Map<String, String> taskWorkDirs = new ConcurrentHashMap<>();

    /** Mapper id to fail. */
    private static volatile int failMapperId;

    /** Test param. */
    private static final String MAP_WRITE = "test.map.write";

    /** {@inheritDoc} */
    @Override public GridGgfsConfiguration ggfsConfiguration() {
        GridGgfsConfiguration cfg = super.ggfsConfiguration();

        cfg.setFragmentizerEnabled(false);

        return cfg;
    }

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
        grid(0).ggfs(ggfsName).format().get();
    }

    /** {@inheritDoc} */
    @Override public GridHadoopConfiguration hadoopConfiguration(String gridName) {
        GridHadoopConfiguration cfg = super.hadoopConfiguration(gridName);

        cfg.setExternalExecution(false);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testMapRun() throws Exception {
        int lineCnt = 10000;
        String fileName = "/testFile";

        prepareFile(fileName, lineCnt);

        totalLineCnt.set(0);
        taskWorkDirs.clear();

        Configuration cfg = new Configuration();

        cfg.setStrings("fs.ggfs.impl", GridGgfsHadoopFileSystem.class.getName());

        Job job = Job.getInstance(cfg);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(TestMapper.class);

        job.setNumReduceTasks(0);

        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("ggfs://:" + getTestGridName(0) + "@/"));
        FileOutputFormat.setOutputPath(job, new Path("ggfs://:" + getTestGridName(0) + "@/output/"));

        job.setJarByClass(getClass());

        GridFuture<?> fut = grid(0).hadoop().submit(new GridHadoopJobId(UUID.randomUUID(), 1),
                createJobInfo(job.getConfiguration()));

        fut.get();

        assertEquals(lineCnt, totalLineCnt.get());

        assertEquals(32, taskWorkDirs.size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testMapCombineRun() throws Exception {
        int lineCnt = 10001;
        String fileName = "/testFile";

        prepareFile(fileName, lineCnt);

        totalLineCnt.set(0);
        taskWorkDirs.clear();

        Configuration cfg = new Configuration();

        cfg.setStrings("fs.ggfs.impl", GridGgfsHadoopFileSystem.class.getName());
        cfg.setBoolean(MAP_WRITE, true);

        Job job = Job.getInstance(cfg);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(TestMapper.class);
        job.setCombinerClass(TestCombiner.class);
        job.setReducerClass(TestReducer.class);

        job.setNumReduceTasks(2);

        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("ggfs://:" + getTestGridName(0) + "@/"));
        FileOutputFormat.setOutputPath(job, new Path("ggfs://:" + getTestGridName(0) + "@/output"));

        job.setJarByClass(getClass());

        GridHadoopJobId jobId = new GridHadoopJobId(UUID.randomUUID(), 2);

        GridFuture<?> fut = grid(0).hadoop().submit(jobId, createJobInfo(job.getConfiguration()));

        fut.get();

        assertEquals(lineCnt, totalLineCnt.get());

        assertEquals(34, taskWorkDirs.size());

        for (int g = 0; g < gridCount(); g++)
            grid(g).hadoop().finishFuture(jobId).get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMapperException() throws Exception {
        prepareFile("/testFile", 1000);

        Configuration cfg = new Configuration();

        cfg.setStrings("fs.ggfs.impl", GridGgfsHadoopFileSystem.class.getName());

        Job job = Job.getInstance(cfg);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(FailMapper.class);

        job.setNumReduceTasks(0);

        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("ggfs://:" + getTestGridName(0) + "@/"));
        FileOutputFormat.setOutputPath(job, new Path("ggfs://:" + getTestGridName(0) + "@/output/"));

        job.setJarByClass(getClass());

        final GridFuture<?> fut = grid(0).hadoop().submit(new GridHadoopJobId(UUID.randomUUID(), 3),
                createJobInfo(job.getConfiguration()));

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                fut.get();

                return null;
            }
        }, GridException.class, null);
    }

    /**
     * @param fileName File name.
     * @param lineCnt Line count.
     * @throws Exception If failed.
     */
    private void prepareFile(String fileName, int lineCnt) throws Exception {
        GridGgfs ggfs = grid(0).ggfs(ggfsName);

        try (OutputStream os = ggfs.create(new GridGgfsPath(fileName), true)) {
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
        prepareFile("/testFile", 5000);

        executedTasks.set(0);
        cancelledTasks.set(0);
        failMapperId = 0;

        Configuration cfg = new Configuration();

        setupFileSystems(cfg);

        Job job = Job.getInstance(cfg);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(CancellingTestMapper.class);

        job.setNumReduceTasks(0);

        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("ggfs://:" + getTestGridName(0) + "@/"));
        FileOutputFormat.setOutputPath(job, new Path("ggfs://:" + getTestGridName(0) + "@/output/"));

        job.setJarByClass(getClass());

        return job.getConfiguration();
    }

    /**
     * @throws Exception If failed.
     */
    public void testTaskCancelling() throws Exception {
        Configuration cfg = prepareJobForCancelling();

        GridHadoopJobId jobId = new GridHadoopJobId(UUID.randomUUID(), 1);

        final GridFuture<?> fut = grid(0).hadoop().submit(jobId, createJobInfo(cfg));

        if (!GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return executedTasks.get() == 16;
            }
        }, 20000)) {
            U.dumpThreads(log);

            assertTrue(false);
        }

        // Fail mapper with id "1", cancels others
        failMapperId = 1;

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                fut.get();

                return null;
            }
        }, GridException.class, null);

        assertEquals(executedTasks.get(), cancelledTasks.get() + 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testJobKill() throws Exception {
        Configuration cfg = prepareJobForCancelling();

        GridHadoop hadoop = grid(0).hadoop();

        GridHadoopJobId jobId = new GridHadoopJobId(UUID.randomUUID(), 1);

        //Kill unknown job.
        boolean killRes = hadoop.kill(jobId);

        assertFalse(killRes);

        final GridFuture<?> fut = hadoop.submit(jobId, createJobInfo(cfg));

        if (!GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                X.println("___ executed tasks: " + executedTasks.get());

                return executedTasks.get() == 16;
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
        }, GridException.class, null);

        assertEquals(executedTasks.get(), cancelledTasks.get());

        //Kill the same job again.
        killRes = hadoop.kill(jobId);

        assertTrue(killRes);
    }

    private static class CancellingTestMapper extends Mapper<Object, Text, Text, IntWritable> {
        private int mapperId;

        /** {@inheritDoc} */
        @Override protected void setup(Context context) throws IOException, InterruptedException {
            mapperId = executedTasks.incrementAndGet();
        }

        /** {@inheritDoc} */
        @Override public void run(Context context) throws IOException, InterruptedException {
            try {
                super.run(context);
            }
            catch (GridHadoopTaskCancelledException e) {
                cancelledTasks.incrementAndGet();

                throw e;
            }
        }

        /** {@inheritDoc} */
        @Override protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            if (mapperId == failMapperId)
                throw new IOException();

            Thread.sleep(1000);
        }
    }

    /**
     * Test failing mapper.
     */
    private static class FailMapper extends Mapper<Object, Text, Text, IntWritable> {
        /** {@inheritDoc} */
        @Override protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
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
        @Override protected void setup(Context context) throws IOException, InterruptedException {
            X.println("___ Mapper: " + context.getTaskAttemptID());

            String taskId = context.getTaskAttemptID().toString();
            String workDir = FileSystem.getLocal(context.getConfiguration()).getWorkingDirectory().toString();

            assertNull(taskWorkDirs.put(workDir, taskId));
        }

        /** {@inheritDoc} */
        @Override protected void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
            if (ctx.getConfiguration().getBoolean(MAP_WRITE, false))
                ctx.write(LINE_COUNT, ONE);
            else
                totalLineCnt.incrementAndGet();
        }
    }

    /**
     * Combiner calculates number of lines.
     */
    private static class TestCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        /** */
        IntWritable sum = new IntWritable();

        /** {@inheritDoc} */
        @Override protected void setup(Context context) throws IOException, InterruptedException {
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
        @Override protected void setup(Context context) throws IOException, InterruptedException {
            X.println("___ Reducer: " + context.getTaskAttemptID());

            String taskId = context.getTaskAttemptID().toString();
            String workDir = FileSystem.getLocal(context.getConfiguration()).getWorkingDirectory().toString();

            assertNull(taskWorkDirs.put(workDir, taskId));
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

            totalLineCnt.addAndGet(lineCnt);
        }
    }
}
