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
import org.apache.hadoop.mapreduce.protocol.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.hadoop.client.*;
import org.gridgain.grid.kernal.processors.hadoop.example_client.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;

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
    @Override protected void afterTest() throws Exception {
        GridHadoopClientProtocolProvider.closeAll();

        super.afterTest();
    }

    /**
     * Test client caching,
     *
     * @throws Exception If failed.
     */
    public void testClientCache() throws Exception {
        GridHadoopClientProtocolProvider provider = provider();

        checkProviderCacheSize(provider, 0);

        Configuration conf1 = config(REST_PORT);
        Configuration conf2 = config(REST_PORT + 1);

        ClientProtocol proto1_1 = provider.create(conf1);

        checkProviderCacheSize(provider, 1);

        ClientProtocol proto2_1 = provider.create(conf2);

        checkProviderCacheSize(provider, 2);

        ClientProtocol proto1_2 = provider.create(conf1);

        checkProviderCacheSize(provider, 2);

        provider.close(proto1_2);

        checkProviderCacheSize(provider, 2);

        provider.close(proto2_1);

        checkProviderCacheSize(provider, 1);

        provider.close(proto1_1);

        checkProviderCacheSize(provider, 0);
    }

    /**
     * Test next job ID generation.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    public void testNextJobId() throws Exception {
        GridHadoopClientProtocolProvider provider = provider();

        ClientProtocol proto = provider.create(config(REST_PORT));

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
        GridGgfs ggfs = grid(0).ggfs(ggfsName);

        ggfs.mkdirs(new GridGgfsPath(PATH_INPUT));

        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(ggfs.create(
            new GridGgfsPath(PATH_INPUT + "/test.file"), true)))) {

            bw.write("word");
        }

        Configuration conf = config(REST_PORT);

        Job job = Job.getInstance(conf);

        job.setUser(USR);
        job.setJobName(JOB_NAME);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(TestMapper.class);
        job.setCombinerClass(TestReducer.class);
        job.setReducerClass(TestReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(PATH_INPUT));
        FileOutputFormat.setOutputPath(job, new Path(PATH_OUTPUT));

        job.submit();

        JobID jobId = job.getJobID();

        checkJobStatus(job.getStatus(), jobId, JOB_NAME, USR, JobStatus.State.RUNNING, 1.0f, 0.0f, 0.0f, 0.0f);

        mapLatch.countDown();

        U.sleep(200);

        checkJobStatus(job.getStatus(), jobId, JOB_NAME, USR, JobStatus.State.RUNNING, 1.0f, 1.0f, 0.0f, 0.0f);

        reduceLatch.countDown();

        U.sleep(200);

        checkJobStatus(job.getStatus(), jobId, JOB_NAME, USR, JobStatus.State.SUCCEEDED, 1.0f, 1.0f, 1.0f, 1.0f);
    }

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
     * Check provider cache size.
     *
     * @param provider Provider.
     * @param expSize Epxected size.
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    private static void checkProviderCacheSize(GridHadoopClientProtocolProvider provider, int expSize)
        throws Exception {
        int res = GridTestUtils.invoke(provider, "activeClients");

        assert res == expSize;
    }

    /**
     * @return Configuration.
     */
    private Configuration config(int port) {
        Configuration conf = new Configuration();

        conf.set(MRConfig.FRAMEWORK_NAME, GridHadoopClientProtocol.PROP_FRAMEWORK_NAME);
        conf.set(GridHadoopClientProtocol.PROP_SRV_ADDR, "127.0.0.1:" + port);

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
