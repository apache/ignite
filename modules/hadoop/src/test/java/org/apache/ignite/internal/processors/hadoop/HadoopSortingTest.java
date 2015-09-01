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

package org.apache.ignite.internal.processors.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.JavaSerialization;
import org.apache.hadoop.io.serializer.JavaSerializationComparator;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.ignite.configuration.HadoopConfiguration;
import org.apache.ignite.internal.util.typedef.X;

import static org.apache.ignite.internal.processors.hadoop.HadoopUtils.createJobInfo;

/**
 * Tests correct sorting.
 */
public class HadoopSortingTest extends HadoopAbstractSelfTest {
    /** */
    private static final String PATH_INPUT = "/test-in";

    /** */
    private static final String PATH_OUTPUT = "/test-out";

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /**
     * @return {@code True} if IGFS is enabled on Hadoop nodes.
     */
    @Override protected boolean igfsEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrids(gridCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);
    }

    /** {@inheritDoc} */
    @Override public HadoopConfiguration hadoopConfiguration(String gridName) {
        HadoopConfiguration cfg = super.hadoopConfiguration(gridName);

        // TODO: IGNITE-404: Uncomment when fixed.
        //cfg.setExternalExecution(false);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSortSimple() throws Exception {
        // Generate test data.
        Job job = Job.getInstance();

        job.setInputFormatClass(InFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapperClass(Mapper.class);
        job.setNumReduceTasks(0);

        setupFileSystems(job.getConfiguration());

        FileOutputFormat.setOutputPath(job, new Path(igfsScheme() + PATH_INPUT));

        X.printerrln("Data generation started.");

        grid(0).hadoop().submit(new HadoopJobId(UUID.randomUUID(), 1),
            createJobInfo(job.getConfiguration())).get(180000);

        X.printerrln("Data generation complete.");

        // Run main map-reduce job.
        job = Job.getInstance();

        setupFileSystems(job.getConfiguration());

        job.getConfiguration().set(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY, JavaSerialization.class.getName() +
            "," + WritableSerialization.class.getName());

        FileInputFormat.setInputPaths(job, new Path(igfsScheme() + PATH_INPUT));
        FileOutputFormat.setOutputPath(job, new Path(igfsScheme() + PATH_OUTPUT));

        job.setSortComparatorClass(JavaSerializationComparator.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setNumReduceTasks(2);

        job.setMapOutputKeyClass(UUID.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        X.printerrln("Job started.");

        grid(0).hadoop().submit(new HadoopJobId(UUID.randomUUID(), 2),
            createJobInfo(job.getConfiguration())).get(180000);

        X.printerrln("Job complete.");

        // Check result.
        Path outDir = new Path(igfsScheme() + PATH_OUTPUT);

        AbstractFileSystem fs = AbstractFileSystem.get(new URI(igfsScheme()), job.getConfiguration());

        for (FileStatus file : fs.listStatus(outDir)) {
            X.printerrln("__ file: " + file);

            if (file.getLen() == 0)
                continue;

            FSDataInputStream in = fs.open(file.getPath());

            Scanner sc = new Scanner(in);

            UUID prev = null;

            while(sc.hasNextLine()) {
                UUID next = UUID.fromString(sc.nextLine());

//                X.printerrln("___ check: " + next);

                if (prev != null)
                    assertTrue(prev.compareTo(next) < 0);

                prev = next;
            }
        }
    }

    public static class InFormat extends InputFormat<Text, NullWritable> {
        /** {@inheritDoc} */
        @Override public List<InputSplit> getSplits(JobContext ctx) throws IOException, InterruptedException {
            List<InputSplit> res = new ArrayList<>();

            FakeSplit split = new FakeSplit(20);

            for (int i = 0; i < 10; i++)
                res.add(split);

            return res;
        }

        /** {@inheritDoc} */
        @Override public RecordReader<Text, NullWritable> createRecordReader(final InputSplit split,
            TaskAttemptContext ctx) throws IOException, InterruptedException {
            return new RecordReader<Text, NullWritable>() {
                /** */
                int cnt;

                /** */
                Text txt = new Text();

                @Override public void initialize(InputSplit split, TaskAttemptContext ctx) {
                    // No-op.
                }

                @Override public boolean nextKeyValue() throws IOException, InterruptedException {
                    return ++cnt <= split.getLength();
                }

                @Override public Text getCurrentKey() {
                    txt.set(UUID.randomUUID().toString());

//                    X.printerrln("___ read: " + txt);

                    return txt;
                }

                @Override public NullWritable getCurrentValue() {
                    return NullWritable.get();
                }

                @Override public float getProgress() throws IOException, InterruptedException {
                    return (float)cnt / split.getLength();
                }

                @Override public void close() {
                    // No-op.
                }
            };
        }
    }

    public static class MyMapper extends Mapper<LongWritable, Text, UUID, NullWritable> {
        /** {@inheritDoc} */
        @Override protected void map(LongWritable key, Text val, Context ctx) throws IOException, InterruptedException {
//            X.printerrln("___ map: " + val);

            ctx.write(UUID.fromString(val.toString()), NullWritable.get());
        }
    }

    public static class MyReducer extends Reducer<UUID, NullWritable, Text, NullWritable> {
        /** */
        private Text text = new Text();

        /** {@inheritDoc} */
        @Override protected void reduce(UUID key, Iterable<NullWritable> vals, Context ctx)
            throws IOException, InterruptedException {
//            X.printerrln("___ rdc: " + key);

            text.set(key.toString());

            ctx.write(text, NullWritable.get());
        }
    }

    public static class FakeSplit extends InputSplit implements Writable {
        /** */
        private static final String[] HOSTS = {"127.0.0.1"};

        /** */
        private int len;

        /**
         * @param len Length.
         */
        public FakeSplit(int len) {
            this.len = len;
        }

        /**
         *
         */
        public FakeSplit() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public long getLength() throws IOException, InterruptedException {
            return len;
        }

        /** {@inheritDoc} */
        @Override public String[] getLocations() throws IOException, InterruptedException {
            return HOSTS;
        }

        /** {@inheritDoc} */
        @Override public void write(DataOutput out) throws IOException {
            out.writeInt(len);
        }

        /** {@inheritDoc} */
        @Override public void readFields(DataInput in) throws IOException {
            len = in.readInt();
        }
    }
}