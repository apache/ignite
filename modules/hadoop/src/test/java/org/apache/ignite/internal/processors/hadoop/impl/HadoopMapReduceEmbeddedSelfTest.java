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

import java.util.UUID;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.ignite.configuration.HadoopConfiguration;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.hadoop.HadoopJobId;
import org.apache.ignite.internal.processors.hadoop.HadoopJobProperty;
import org.apache.ignite.internal.processors.hadoop.impl.examples.HadoopWordCount1;
import org.apache.ignite.internal.processors.hadoop.impl.examples.HadoopWordCount2;

import static org.apache.ignite.internal.processors.hadoop.impl.HadoopUtils.createJobInfo;
import static org.apache.ignite.internal.processors.hadoop.state.HadoopMapReduceEmbeddedSelfTestState.flags;

/**
 * Tests map-reduce execution with embedded mode.
 */
public class HadoopMapReduceEmbeddedSelfTest extends HadoopMapReduceTest {
    /** {@inheritDoc} */
    @Override public HadoopConfiguration hadoopConfiguration(String gridName) {
        HadoopConfiguration cfg = super.hadoopConfiguration(gridName);

        // TODO: IGNITE-404: Uncomment when fixed.
        //cfg.setExternalExecution(false);

        return cfg;
    }

    /**
     * @throws Exception If fails.
     */
    public void testMultiReducerWholeMapReduceExecution() throws Exception {
        checkMultiReducerWholeMapReduceExecution(false);
    }

    /**
     * @throws Exception If fails.
     */
    public void testMultiReducerWholeMapReduceExecutionStriped() throws Exception {
        checkMultiReducerWholeMapReduceExecution(true);
    }

    /**
     * Tests whole job execution with all phases in old and new versions of API with definition of custom
     * Serialization, Partitioner and IO formats.
     *
     * @param striped Whether output should be striped or not.
     * @throws Exception If fails.
     */
    public void checkMultiReducerWholeMapReduceExecution(boolean striped) throws Exception {
        IgfsPath inDir = new IgfsPath(PATH_INPUT);

        igfs.mkdirs(inDir);

        IgfsPath inFile = new IgfsPath(inDir, HadoopWordCount2.class.getSimpleName() + "-input");

        generateTestFile(inFile.toString(), "key1", 10000, "key2", 20000, "key3", 15000, "key4", 7000, "key5", 12000,
            "key6", 18000 );

        for (int i = 0; i < 2; i++) {
            boolean useNewAPI = i == 1;

            igfs.delete(new IgfsPath(PATH_OUTPUT), true);

            flags.put("serializationWasConfigured", false);
            flags.put("partitionerWasConfigured", false);
            flags.put("inputFormatWasConfigured", false);
            flags.put("outputFormatWasConfigured", false);

            JobConf jobConf = new JobConf();

            if (striped)
                jobConf.set(HadoopJobProperty.SHUFFLE_MAPPER_STRIPED_OUTPUT.propertyName(), "true");
            else
                jobConf.set(HadoopJobProperty.SHUFFLE_MAPPER_STRIPED_OUTPUT.propertyName(), "false");

            jobConf.set(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY, CustomSerialization.class.getName());

            //To split into about 6-7 items for v2
            jobConf.setInt(FileInputFormat.SPLIT_MAXSIZE, 65000);

            //For v1
            jobConf.setInt("fs.local.block.size", 65000);

            // File system coordinates.
            setupFileSystems(jobConf);

            HadoopWordCount1.setTasksClasses(jobConf, !useNewAPI, !useNewAPI, !useNewAPI);

            if (!useNewAPI) {
                jobConf.setPartitionerClass(CustomV1Partitioner.class);
                jobConf.setInputFormat(CustomV1InputFormat.class);
                jobConf.setOutputFormat(CustomV1OutputFormat.class);
            }

            Job job = Job.getInstance(jobConf);

            HadoopWordCount2.setTasksClasses(job, useNewAPI, useNewAPI, useNewAPI, false);

            if (useNewAPI) {
                job.setPartitionerClass(CustomV2Partitioner.class);
                job.setInputFormatClass(CustomV2InputFormat.class);
                job.setOutputFormatClass(CustomV2OutputFormat.class);
            }

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.setInputPaths(job, new Path(igfsScheme() + inFile.toString()));
            FileOutputFormat.setOutputPath(job, new Path(igfsScheme() + PATH_OUTPUT));

            job.setNumReduceTasks(3);

            job.setJarByClass(HadoopWordCount2.class);

            IgniteInternalFuture<?> fut = grid(0).hadoop().submit(new HadoopJobId(UUID.randomUUID(), 1),
                    createJobInfo(job.getConfiguration()));

            fut.get();

            assertTrue("Serialization was configured (new API is " + useNewAPI + ")",
                 flags.get("serializationWasConfigured"));

            assertTrue("Partitioner was configured (new API is = " + useNewAPI + ")",
                 flags.get("partitionerWasConfigured"));

            assertTrue("Input format was configured (new API is = " + useNewAPI + ")",
                 flags.get("inputFormatWasConfigured"));

            assertTrue("Output format was configured (new API is = " + useNewAPI + ")",
                 flags.get("outputFormatWasConfigured"));

            assertEquals("Use new API = " + useNewAPI,
                "key3\t15000\n" +
                "key6\t18000\n",
                readAndSortFile(PATH_OUTPUT + "/" + (useNewAPI ? "part-r-" : "part-") + "00000")
            );

            assertEquals("Use new API = " + useNewAPI,
                "key1\t10000\n" +
                "key4\t7000\n",
                readAndSortFile(PATH_OUTPUT + "/" + (useNewAPI ? "part-r-" : "part-") + "00001")
            );

            assertEquals("Use new API = " + useNewAPI,
                "key2\t20000\n" +
                "key5\t12000\n",
                readAndSortFile(PATH_OUTPUT + "/" + (useNewAPI ? "part-r-" : "part-") + "00002")
            );

        }
    }

    /**
     * Custom serialization class that inherits behaviour of native {@link WritableSerialization}.
     */
    protected static class CustomSerialization extends WritableSerialization {
        @Override public void setConf(Configuration conf) {
            super.setConf(conf);

            flags.put("serializationWasConfigured", true);
        }
    }

    /**
     * Custom implementation of Partitioner in v1 API.
     */
    private static class CustomV1Partitioner extends org.apache.hadoop.mapred.lib.HashPartitioner {
        /** {@inheritDoc} */
        @Override public void configure(JobConf job) {
            flags.put("partitionerWasConfigured", true);
        }
    }

    /**
     * Custom implementation of Partitioner in v2 API.
     */
    private static class CustomV2Partitioner extends org.apache.hadoop.mapreduce.lib.partition.HashPartitioner
            implements Configurable {
        /** {@inheritDoc} */
        @Override public void setConf(Configuration conf) {
            flags.put("partitionerWasConfigured", true);
        }

        /** {@inheritDoc} */
        @Override public Configuration getConf() {
            return null;
        }
    }

    /**
     * Custom implementation of InputFormat in v2 API.
     */
    private static class CustomV2InputFormat extends org.apache.hadoop.mapreduce.lib.input.TextInputFormat implements Configurable {
        /** {@inheritDoc} */
        @Override public void setConf(Configuration conf) {
            flags.put("inputFormatWasConfigured", true);
        }

        /** {@inheritDoc} */
        @Override public Configuration getConf() {
            return null;
        }
    }

    /**
     * Custom implementation of OutputFormat in v2 API.
     */
    private static class CustomV2OutputFormat extends org.apache.hadoop.mapreduce.lib.output.TextOutputFormat implements Configurable {
        /** {@inheritDoc} */
        @Override public void setConf(Configuration conf) {
            flags.put("outputFormatWasConfigured", true);
        }

        /** {@inheritDoc} */
        @Override public Configuration getConf() {
            return null;
        }
    }

    /**
     * Custom implementation of InputFormat in v1 API.
     */
    private static class CustomV1InputFormat extends org.apache.hadoop.mapred.TextInputFormat {
        /** {@inheritDoc} */
        @Override public void configure(JobConf job) {
            super.configure(job);

            flags.put("inputFormatWasConfigured", true);
        }
    }

    /**
     * Custom implementation of OutputFormat in v1 API.
     */
    private static class CustomV1OutputFormat extends org.apache.hadoop.mapred.TextOutputFormat implements JobConfigurable {
        /** {@inheritDoc} */
        @Override public void configure(JobConf job) {
            flags.put("outputFormatWasConfigured", true);
        }
    }
}