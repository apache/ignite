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
import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.ggfs.hadoop.v1.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.hadoop.*;

import java.io.*;
import java.util.*;

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

        cfg.setBoolean("gridgain.hadoop.external_execution", true);

        Job job = Job.getInstance(cfg);

        job.setMapperClass(TestMapper.class);
        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path("ggfs://ipc/" + testInputFile));

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
        @Override protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println(">>> Mapper read value: " + value);
        }
    }
}
