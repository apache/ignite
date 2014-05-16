/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.ggfs.hadoop.v1.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.hadoop.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.hadoop.GridHadoopJobProperty.*;

/**
 * Job tracker self test.
 */
public class GridHadoopExternalTaskExecutionSelfTest extends GridHadoopAbstractSelfTest {
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
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleTaskSubmit() throws Exception {
        String testInputFile = "/test";

        prepareTestFile(testInputFile);

        Configuration cfg = new Configuration();

        cfg.set("fs.ggfs.impl", GridGgfsHadoopFileSystem.class.getName());

        cfg.setBoolean(EXTERNAL_EXECUTION.propertyName(), true);

        Job job = Job.getInstance(cfg);

        job.setMapperClass(TestMapper.class);
        job.setCombinerClass(TestReducer.class);
        job.setReducerClass(TestReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, new Path("ggfs://ipc/" + testInputFile));
        FileOutputFormat.setOutputPath(job, new Path("ggfs://ipc/output"));

        job.setJarByClass(getClass());

        GridHadoopProcessorAdapter hadoop = ((GridKernal)grid(0)).context().hadoop();

        GridFuture<?> fut = hadoop.submit(new GridHadoopJobId(UUID.randomUUID(), 1),
            new GridHadoopDefaultJobInfo(job.getConfiguration()));

        fut.get();
    }

    /**
     * @param filePath File path to prepare.
     * @throws Exception If failed.
     */
    private void prepareTestFile(String filePath) throws Exception {
        GridGgfs ggfs = grid(0).ggfs(ggfsName);

        try (GridGgfsOutputStream out = ggfs.create(new GridGgfsPath(filePath), true)) {
            PrintWriter wr = new PrintWriter(new OutputStreamWriter(out));

            for (int i = 0; i < 1000; i++)
                wr.println("Hello, world: " + i);

            wr.flush();
        }
    }

    /**
     *
     */
    private static class TestMapper extends Mapper<Object, Text, Text, IntWritable> {
        /** One constant. */
        private IntWritable one = new IntWritable(1);

        /** Line constant. */
        private Text line = new Text("line");

        @Override protected void map(Object key, Text val, Context ctx) throws IOException, InterruptedException {
            ctx.write(line, one);

            System.out.println(">>>> Mapped: " + val);
        }
    }

    /**
     *
     */
    private static class TestReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        /** Line constant. */
        private Text line = new Text("line");

        @Override protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            System.out.println(">>> Reducer setup.");
        }

        /** {@inheritDoc} */
        @Override protected void reduce(Text key, Iterable<IntWritable> values, Context ctx)
            throws IOException, InterruptedException {
            int s = 0;

            for (IntWritable val : values) {
                System.out.println("Reducing [key=" + key + ", val=" + val.get() + ']');

                s += val.get();
            }

            System.out.println(">>>> Reduced: " + s);

            ctx.write(line, new IntWritable(s));
        }
    }
}
