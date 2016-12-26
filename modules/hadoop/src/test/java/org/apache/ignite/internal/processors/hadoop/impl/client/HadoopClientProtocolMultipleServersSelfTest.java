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

package org.apache.ignite.internal.processors.hadoop.impl.client;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.hadoop.mapreduce.IgniteHadoopClientProtocolProvider;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.client.GridServerUnreachableException;
import org.apache.ignite.internal.processors.hadoop.impl.HadoopAbstractSelfTest;
import org.apache.ignite.internal.processors.hadoop.impl.HadoopUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Hadoop client protocol configured with multiple ignite servers tests.
 */
@SuppressWarnings("ResultOfMethodCallIgnored")
public class HadoopClientProtocolMultipleServersSelfTest extends HadoopAbstractSelfTest {
    /** Input path. */
    private static final String PATH_INPUT = "/input";

    /** Job name. */
    private static final String JOB_NAME = "myJob";

    /** Rest port. */
    private static int restPort;

    /** {@inheritDoc} */
    @Override protected boolean igfsEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected boolean restEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.getConnectorConfiguration().setPort(restPort++);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    private void beforeJob() throws Exception {
        IgniteFileSystem igfs = grid(0).fileSystem(HadoopAbstractSelfTest.igfsName);

        igfs.format();

        igfs.mkdirs(new IgfsPath(PATH_INPUT));

        try (BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(igfs.create(
            new IgfsPath(PATH_INPUT + "/test.file"), true)))) {

            bw.write("word");
        }
    }

    /**
     * Test job submission.
     *
     * @param conf Hadoop configuration.
     * @throws Exception If failed.
     */
    private void checkJobSubmit(Configuration conf) throws Exception {
        final Job job = Job.getInstance(conf);

        try {
            job.setJobName(JOB_NAME);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(OutFormat.class);

            job.setMapperClass(TestMapper.class);
            job.setReducerClass(TestReducer.class);

            job.setNumReduceTasks(0);

            FileInputFormat.setInputPaths(job, new Path(PATH_INPUT));

            job.submit();

            job.waitForCompletion(false);

            assert job.getStatus().getState() == JobStatus.State.SUCCEEDED : job.getStatus().getState();
        }
        finally {
            job.getCluster().close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    public void testMultipleAddresses() throws Exception {
        restPort = REST_PORT;

        startGrids(gridCount());

        beforeJob();

        U.sleep(5000);

        checkJobSubmit(configMultipleAddrs(gridCount()));
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"ConstantConditions", "ThrowableResultOfMethodCallIgnored"})
    public void testSingleAddress() throws Exception {
        try {
            // Don't use REST_PORT to test connection fails if the only this port is configured
            restPort = REST_PORT + 1;

            startGrids(gridCount());

            GridTestUtils.assertThrowsAnyCause(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                        checkJobSubmit(configSingleAddress());
                        return null;
                    }
                },
                GridServerUnreachableException.class, "Failed to connect to any of the servers in list");
        }
        finally {
            FileSystem fs = FileSystem.get(configSingleAddress());

            fs.close();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    public void testMixedAddrs() throws Exception {
        restPort = REST_PORT;

        startGrids(gridCount());

        beforeJob();

        stopGrid(1);

        U.sleep(5000);

        checkJobSubmit(configMixed());

        startGrid(1);

        awaitPartitionMapExchange();
    }

    /**
     * @return Configuration.
     */
    private Configuration configSingleAddress() {
        Configuration conf = HadoopUtils.safeCreateConfiguration();

        setupFileSystems(conf);

        conf.set(MRConfig.FRAMEWORK_NAME, IgniteHadoopClientProtocolProvider.FRAMEWORK_NAME);
        conf.set(MRConfig.MASTER_ADDRESS, "127.0.0.1:" + REST_PORT);

        conf.set("fs.defaultFS", "igfs:///");

        return conf;
    }

    /**
     * @param srvsCnt Count ov servers.
     * @return Configuration.
     */
    private Configuration configMultipleAddrs(int srvsCnt) {
        Configuration conf = HadoopUtils.safeCreateConfiguration();

        setupFileSystems(conf);

        conf.set(MRConfig.FRAMEWORK_NAME, IgniteHadoopClientProtocolProvider.FRAMEWORK_NAME);

        Collection<String> addrs = new ArrayList<>(srvsCnt);

        for (int i = 0; i < srvsCnt; ++i)
            addrs.add("127.0.0.1:" + Integer.toString(REST_PORT + i));

        conf.set(MRConfig.MASTER_ADDRESS, F.concat(addrs, ","));

        conf.set("fs.defaultFS", "igfs:///");

        return conf;
    }

    /**
     * @return Configuration.
     */
    private Configuration configMixed() {
        Configuration conf = HadoopUtils.safeCreateConfiguration();

        setupFileSystems(conf);

        conf.set(MRConfig.FRAMEWORK_NAME, IgniteHadoopClientProtocolProvider.FRAMEWORK_NAME);

        Collection<String> addrs = new ArrayList<>();

        addrs.add("localhost");
        addrs.add("127.0.0.1:" + Integer.toString(REST_PORT + 1));

        conf.set(MRConfig.MASTER_ADDRESS, F.concat(addrs, ","));

        conf.set("fs.defaultFS", "igfs:///");

        return conf;
    }

    /**
     * Test mapper.
     */
    public static class TestMapper extends Mapper<Object, Text, Text, IntWritable> {
        /** {@inheritDoc} */
        @Override public void map(Object key, Text val, Context ctx) throws IOException, InterruptedException {
            // No-op.
        }
    }

    /**
     * Test reducer.
     */
    public static class TestReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        /** {@inheritDoc} */
        @Override public void reduce(Text key, Iterable<IntWritable> values, Context ctx) throws IOException,
            InterruptedException {
            // No-op.
        }
    }

    /**
     * Test output formatter.
     */
    public static class OutFormat extends OutputFormat {
        /** {@inheritDoc} */
        @Override public RecordWriter getRecordWriter(TaskAttemptContext ctx) throws IOException,
            InterruptedException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void checkOutputSpecs(JobContext ctx) throws IOException, InterruptedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public OutputCommitter getOutputCommitter(TaskAttemptContext ctx) throws IOException,
            InterruptedException {
            return null;
        }
    }
}