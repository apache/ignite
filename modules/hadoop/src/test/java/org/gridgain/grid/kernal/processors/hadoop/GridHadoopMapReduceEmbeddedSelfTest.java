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
import org.apache.hadoop.io.serializer.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.examples.*;

import java.util.*;

import static org.gridgain.grid.kernal.processors.hadoop.GridHadoopUtils.*;

/**
 * Tests map-reduce execution with embedded mode.
 */
public class GridHadoopMapReduceEmbeddedSelfTest extends GridHadoopMapReduceTest {
    /** */
    private static boolean partitionerWasConfigured;

    /** */
    private static boolean serializationWasConfigured;

    /** */
    private static boolean inputFormatWasConfigured;

    /** */
    private static boolean outputFormatWasConfigured;

    /** {@inheritDoc} */
    @Override public GridHadoopConfiguration hadoopConfiguration(String gridName) {
        GridHadoopConfiguration cfg = super.hadoopConfiguration(gridName);

        cfg.setExternalExecution(false);

        return cfg;
    }

    /**
     * Tests whole job execution with all phases in old and new versions of API with definition of custom
     * Serialization, Partitioner and IO formats.
     * @throws Exception If fails.
     */
    public void testMultiReducerWholeMapReduceExecution() throws Exception {
        GridGgfsPath inDir = new GridGgfsPath(PATH_INPUT);

        ggfs.mkdirs(inDir);

        GridGgfsPath inFile = new GridGgfsPath(inDir, GridHadoopWordCount2.class.getSimpleName() + "-input");

        generateTestFile(inFile.toString(), "key1", 10000, "key2", 20000, "key3", 15000, "key4", 7000, "key5", 12000,
                "key6", 18000 );

        for (int i = 0; i < 2; i++) {
            boolean useNewAPI = i == 1;

            ggfs.delete(new GridGgfsPath(PATH_OUTPUT), true);

            serializationWasConfigured = false;
            partitionerWasConfigured = false;
            inputFormatWasConfigured = false;
            outputFormatWasConfigured = false;

            JobConf jobConf = new JobConf();

            jobConf.set(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY, CustomSerialization.class.getName());

            //To split into about 6-7 items for v2
            jobConf.setInt(FileInputFormat.SPLIT_MAXSIZE, 65000);

            //For v1
            jobConf.setInt("fs.local.block.size", 65000);

            // File system coordinates.
            setupFileSystems(jobConf);

            GridHadoopWordCount1.setTasksClasses(jobConf, !useNewAPI, !useNewAPI, !useNewAPI);

            if (!useNewAPI) {
                jobConf.setPartitionerClass(CustomV1Partitioner.class);
                jobConf.setInputFormat(CustomV1InputFormat.class);
                jobConf.setOutputFormat(CustomV1OutputFormat.class);
            }

            Job job = Job.getInstance(jobConf);

            GridHadoopWordCount2.setTasksClasses(job, useNewAPI, useNewAPI, useNewAPI);

            if (useNewAPI) {
                job.setPartitionerClass(CustomV2Partitioner.class);
                job.setInputFormatClass(CustomV2InputFormat.class);
                job.setOutputFormatClass(CustomV2OutputFormat.class);
            }

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.setInputPaths(job, new Path(ggfsScheme() + inFile.toString()));
            FileOutputFormat.setOutputPath(job, new Path(ggfsScheme() + PATH_OUTPUT));

            job.setNumReduceTasks(3);

            job.setJarByClass(GridHadoopWordCount2.class);

            GridFuture<?> fut = grid(0).hadoop().submit(new GridHadoopJobId(UUID.randomUUID(), 1),
                    createJobInfo(job.getConfiguration()));

            fut.get();

            assertTrue("Serialization was configured (new API is " + useNewAPI + ")", serializationWasConfigured);

            assertTrue("Partitioner was configured (new API is = " + useNewAPI + ")", partitionerWasConfigured);

            assertTrue("Input format was configured (new API is = " + useNewAPI + ")", inputFormatWasConfigured);

            assertTrue("Output format was configured (new API is = " + useNewAPI + ")", outputFormatWasConfigured);

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

            serializationWasConfigured = true;
        }
    }

    /**
     * Custom implementation of Partitioner in v1 API.
     */
    private static class CustomV1Partitioner extends org.apache.hadoop.mapred.lib.HashPartitioner {
        /** {@inheritDoc} */
        @Override public void configure(JobConf job) {
            partitionerWasConfigured = true;
        }
    }

    /**
     * Custom implementation of Partitioner in v2 API.
     */
    private static class CustomV2Partitioner extends org.apache.hadoop.mapreduce.lib.partition.HashPartitioner
            implements Configurable {
        /** {@inheritDoc} */
        @Override public void setConf(Configuration conf) {
            partitionerWasConfigured = true;
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
            inputFormatWasConfigured = true;
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
            outputFormatWasConfigured = true;
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

            inputFormatWasConfigured = true;
        }
    }

    /**
     * Custom implementation of OutputFormat in v1 API.
     */
    private static class CustomV1OutputFormat extends org.apache.hadoop.mapred.TextOutputFormat implements JobConfigurable {
        /** {@inheritDoc} */
        @Override public void configure(JobConf job) {
            outputFormatWasConfigured = true;
        }
    }
}
