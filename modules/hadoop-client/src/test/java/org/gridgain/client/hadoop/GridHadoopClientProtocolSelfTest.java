/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.hadoop;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.protocol.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Hadoop client protocol tests.
 */
public class GridHadoopClientProtocolSelfTest extends GridHadoopAbstractSelfTest {
    /** Input path. */
    private static final String PATH_INPUT = "/input";

    /** Output path. */
    private static final String PATH_OUTPUT = "/output";

    /** User. */
    private static final String USR = "user";

    /** Job name. */
    private static final String JOB_NAME = "myJob";

    /** Map latch. */
    private static CountDownLatch mapLatch;

    /** Reduce latch. */
    private static CountDownLatch reduceLatch;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected boolean ggfsEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected boolean restEnabled() {
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

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        mapLatch = new CountDownLatch(1);
        reduceLatch = new CountDownLatch(1);

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override public GridHadoopConfiguration hadoopConfiguration(String gridName) {
        GridHadoopConfiguration cfg = super.hadoopConfiguration(gridName);

        cfg.setExternalExecution(false);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();
    }

    /**
     * Test next job ID generation.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    public void testNextJobId() throws Exception {
        GridHadoopClientProtocolProvider provider = provider();

        ClientProtocol proto = provider.create(config(GridHadoopAbstractSelfTest.REST_PORT));

        JobID jobId = proto.getNewJobID();

        assert jobId != null;
        assert jobId.getJtIdentifier() != null;

        JobID nextJobId = proto.getNewJobID();

        assert nextJobId != null;
        assert nextJobId.getJtIdentifier() != null;

        assert !F.eq(jobId, nextJobId);
    }

    /**
     * Test job submission.
     *
     * @throws Exception If failed.
     */
    public void testJobSubmit() throws Exception {
        GridGgfs ggfs = grid(0).ggfs(GridHadoopAbstractSelfTest.ggfsName);

        ggfs.mkdirs(new GridGgfsPath(PATH_INPUT));

        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(ggfs.create(
            new GridGgfsPath(PATH_INPUT + "/test.file"), true)))) {

            bw.write("word");
        }

        Configuration conf = config(GridHadoopAbstractSelfTest.REST_PORT);

        final Job job = Job.getInstance(conf);

        job.setUser(USR);
        job.setJobName(JOB_NAME);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(TestMapper.class);
        job.setReducerClass(TestReducer.class);

        // TODO: Remove.
//        job.setNumReduceTasks(0);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(PATH_INPUT));
        FileOutputFormat.setOutputPath(job, new Path(PATH_OUTPUT));

        job.submit();

        JobID jobId = job.getJobID();

        checkJobStatus(job.getStatus(), jobId, JOB_NAME, USR, JobStatus.State.RUNNING, 1.0f, 0.0f, 0.0f, 0.0f);

        mapLatch.countDown();

        long endTime = U.currentTimeMillis() + 5000L;

        while (U.currentTimeMillis() < endTime) {
            if (F.eq(1.0f, job.getStatus().getMapProgress()))
                break;
        }

        assert F.eq(1.0f, job.getStatus().getMapProgress());

        checkJobStatus(job.getStatus(), jobId, JOB_NAME, USR, JobStatus.State.RUNNING, 1.0f, 1.0f, 0.0f, 0.0f);

        reduceLatch.countDown();

        job.waitForCompletion(false);

        checkJobStatus(job.getStatus(), jobId, JOB_NAME, USR, JobStatus.State.SUCCEEDED, 1.0f, 1.0f, 1.0f, 1.0f);
    }

    /**
     * Check job status.
     *
     * @param status Job status.
     * @param expJobId Expected job ID.
     * @param expJobName Expected job name.
     * @param expUser Expected user.
     * @param expState Expected state.
     * @param expSetupProgress Expected setup progress.
     * @param expMapProgress Expected map progress.
     * @param expReduceProgress Expected reduce progress.
     * @param expCleanupProgress Expected cleanup progress.
     * @throws Exception If failed.
     */
    private static void checkJobStatus(JobStatus status, JobID expJobId, String expJobName, String expUser,
        JobStatus.State expState, float expSetupProgress, float expMapProgress, float expReduceProgress,
        float expCleanupProgress) throws Exception {
        assert F.eq(status.getJobID(), expJobId);
        assert F.eq(status.getJobName(), expJobName);
        assert F.eq(status.getUsername(), expUser);
        assert F.eq(status.getState(), expState);

        assert F.eq(status.getSetupProgress(), expSetupProgress);
        assert F.eq(status.getMapProgress(), expMapProgress);
        assert F.eq(status.getReduceProgress(), expReduceProgress);
        assert F.eq(status.getCleanupProgress(), expCleanupProgress);
    }

    /**
     * @return Configuration.
     */
    private Configuration config(int port) {
        Configuration conf = new Configuration();

        conf.set(MRConfig.FRAMEWORK_NAME, GridHadoopClientProtocol.FRAMEWORK_NAME);
        conf.set(MRConfig.MASTER_ADDRESS, "127.0.0.1:" + port);

        conf.set("fs.default.name", "ggfs://ipc");
        conf.set("fs.ggfs.impl", "org.gridgain.grid.ggfs.hadoop.v1.GridGgfsHadoopFileSystem");
        conf.set("fs.AbstractFileSystem.ggfs.impl", "org.gridgain.grid.ggfs.hadoop.v2.GridGgfsHadoopFileSystem");

        return conf;
    }

    /**
     * @return Protocol provider.
     */
    private GridHadoopClientProtocolProvider provider() {
        return new GridHadoopClientProtocolProvider();
    }

    /**
     * Test mapper.
     */
    public static class TestMapper extends Mapper<Object, Text, Text, IntWritable> {
        /** Writable container for writing word. */
        private Text word = new Text();

        /** Writable integer constant of '1' is writing as count of found words. */
        private static final IntWritable one = new IntWritable(1);

        /** {@inheritDoc} */
        @Override public void map(Object key, Text val, Context ctx) throws IOException, InterruptedException {
            mapLatch.await();

            StringTokenizer wordList = new StringTokenizer(val.toString());

            while (wordList.hasMoreTokens()) {
                word.set(wordList.nextToken());
                ctx.write(word, one);
            }
        }
    }

    /**
     * Test reducer.
     */
    public static class TestReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        /** Writable container for writing sum of word counts. */
        private IntWritable totalWordCnt = new IntWritable();

        /** {@inheritDoc} */
        @Override public void reduce(Text key, Iterable<IntWritable> values, Context ctx) throws IOException,
            InterruptedException {
            reduceLatch.await();

            int wordCnt = 0;

            for (IntWritable value : values) {
                wordCnt += value.get();
            }

            totalWordCnt.set(wordCnt);
            ctx.write(key, totalWordCnt);
        }
    }
}
