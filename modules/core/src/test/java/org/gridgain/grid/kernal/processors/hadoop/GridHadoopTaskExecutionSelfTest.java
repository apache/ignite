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
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Tests map-reduce task execution basics.
 */
public class GridHadoopTaskExecutionSelfTest extends GridHadoopAbstractSelfTest {
    /** Line count. */
    private static final AtomicInteger totalLineCnt = new AtomicInteger();

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
        startGrids(gridCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMapRun() throws Exception {
        int lineCnt = 10000;
        String fileName = "/testFile";

        prepareFile(fileName, lineCnt);

        totalLineCnt.set(0);

        Configuration cfg = new Configuration();

        cfg.setStrings("fs.ggfs.impl", GridGgfsHadoopFileSystem.class.getName());

        Job job = Job.getInstance(cfg);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(TestMapper.class);

        job.setNumReduceTasks(0);

        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("ggfs://ipc/"));

        job.setJarByClass(getClass());

        GridHadoopProcessor hadoop = ((GridKernal)grid(0)).context().hadoop();

        GridFuture<?> fut = hadoop.submit(new GridHadoopJobId(UUID.randomUUID(), 1),
            new GridHadoopDefaultJobInfo(job.getConfiguration()));

        fut.get();

        assertEquals(lineCnt, totalLineCnt.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testMapCombineRun() throws Exception {
        int lineCnt = 10000;
        String fileName = "/testFile";

        prepareFile(fileName, lineCnt);

        totalLineCnt.set(0);

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

        FileInputFormat.setInputPaths(job, new Path("ggfs://ipc/"));
        FileOutputFormat.setOutputPath(job, new Path("ggfs://ipc/"));

        job.setJarByClass(getClass());

        GridHadoopProcessor hadoop = ((GridKernal)grid(0)).context().hadoop();

        GridFuture<?> fut = hadoop.submit(new GridHadoopJobId(UUID.randomUUID(), 2),
            new GridHadoopDefaultJobInfo(job.getConfiguration()));

        fut.get();

        assertEquals(lineCnt, totalLineCnt.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testMapperException() throws Exception {
        int lineCnt = 1000;
        String fileName = "/testFile";

        prepareFile(fileName, lineCnt);

        Configuration cfg = new Configuration();

        cfg.setStrings("fs.ggfs.impl", GridGgfsHadoopFileSystem.class.getName());

        Job job = Job.getInstance(cfg);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(FailMapper.class);

        job.setNumReduceTasks(0);

        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("ggfs://ipc/"));

        job.setJarByClass(getClass());

        GridHadoopProcessor hadoop = ((GridKernal)grid(0)).context().hadoop();

        final GridFuture<?> fut = hadoop.submit(new GridHadoopJobId(UUID.randomUUID(), 3),
            new GridHadoopDefaultJobInfo(job.getConfiguration()));

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
                w.println("Hello, Hadoop map-reduce!");

            w.flush();
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
