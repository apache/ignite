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
import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.ggfs.hadoop.v1.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.hadoop.examples.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * TODO: Add class description.
 */
public class GridHadoopMapCombineTaskSelfTest extends GridHadoopAbstractSelfTest {
    /** Line count. */
    private static final AtomicInteger totalLineCnt = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
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
    public void testMapCombineRun() throws Exception {
        int lineCnt = 1000;
        String fileName = "/testFile";

        prepareFile(fileName, lineCnt);

        totalLineCnt.set(0);

        Configuration cfg = new Configuration();

        cfg.setStrings("fs.ggfs.impl", GridGgfsHadoopFileSystem.class.getName());

        Job job = Job.getInstance(cfg);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(TestMapper.class);
        job.setCombinerClass(TestCombiner.class);

        job.setNumReduceTasks(0);

        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.setInputPaths(job, new Path("ggfs://ipc/"));

        job.setJarByClass(GridGainWordCount2.class);

        GridHadoopProcessor hadoop = ((GridKernal)grid(0)).context().hadoop();

        GridFuture<?> fut = hadoop.submit(new GridHadoopJobId(UUID.randomUUID(), 1),
            new GridHadoopDefaultJobInfo(job.getConfiguration()));

        fut.get();

        assertEquals(lineCnt, totalLineCnt.get());
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
     * Mapper calculates number of lines.
     */
    private static class TestMapper extends Mapper<Object, Text, Text, IntWritable> {
        /** Writable integer constant of '1'. */
        private static final IntWritable ONE = new IntWritable(1);

        /** Line count constant. */
        public static final Text LINE_COUNT = new Text("lineCount");

        /** {@inheritDoc} */
        @Override protected void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
            System.out.println("Word!!!");

            ctx.write(LINE_COUNT, ONE);
        }
    }

    /**
     * Combiner calculates number of lines.
     */
    private static class TestCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        /** {@inheritDoc} */
        @Override protected void reduce(Text key, Iterable<IntWritable> values, Context ctx) throws IOException,
            InterruptedException {
            int lineCnt = 0;

            for (IntWritable value : values)
                lineCnt += value.get();

            totalLineCnt.addAndGet(lineCnt);
        }
    }
}
