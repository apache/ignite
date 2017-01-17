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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.ignite.configuration.HadoopConfiguration;
import org.apache.ignite.internal.processors.hadoop.HadoopJobId;
import org.apache.ignite.internal.processors.hadoop.state.HadoopGroupingTestState;

import static org.apache.ignite.internal.processors.hadoop.impl.HadoopUtils.createJobInfo;

/**
 * Grouping test.
 */
public class HadoopCombinerTest extends HadoopAbstractSelfTest {
    /** Splits count. */
    private static final int SPLITS_COUNT = 10;

    /** Split length. */
    private static final int SPLIT_LEN = 20;

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
    public void test() throws Exception {
        HadoopGroupingTestState.values().clear();

        Job job = Job.getInstance();

        job.setInputFormatClass(InFormat.class);
        job.setOutputFormatClass(OutFormat.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(TestMapper.class);

        job.setCombinerClass(TestCombiner.class);
        job.setCombinerKeyGroupingComparatorClass(IntWritable.Comparator.class);

        job.setNumReduceTasks(2);
        job.setReducerClass(TestReducer.class);
        job.setGroupingComparatorClass(IntWritable.Comparator.class);

        grid(0).hadoop().submit(new HadoopJobId(UUID.randomUUID(), 2),
            createJobInfo(job.getConfiguration())).get(30000);
    }

    /**
     *
     */
    public static class TestMapper extends Mapper<IntWritable, Text, IntWritable, IntWritable> {
        @Override protected void map(IntWritable key, Text value,
            Context context) throws IOException, InterruptedException {
//            System.out.println("--- " + Thread.currentThread().getName() + " " + key + " " + value);

            context.write(new IntWritable(key.get() % 2), new IntWritable(1));
        }
    }

    /**
     *
     */
    public static class TestCombiner extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        /** {@inheritDoc} */

        @Override protected void reduce(IntWritable key, Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException {
            int cnt = 0;

            for (Iterator<IntWritable> it = values.iterator(); it.hasNext(); it.next())
                cnt++;

//            System.out.println("+++ " + Thread.currentThread().getName() + " " + key + " " + cnt);
            context.write(key, new IntWritable(cnt));
        }
    }

    /**
     *
     */
    public static class TestReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        /** {@inheritDoc} */
        @Override protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

            int cnt = 0;

            for (Iterator<IntWritable> it = values.iterator(); it.hasNext(); )
                cnt += it.next().get();

            assertEquals(SPLIT_LEN * SPLITS_COUNT / 2, cnt);
        }
    }

    /**
     *
     */
    public static class InFormat extends InputFormat<IntWritable, Text> {
        /** {@inheritDoc} */
        @Override public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
            ArrayList<InputSplit> list = new ArrayList<>();

            for (int i = 0; i < SPLITS_COUNT; i++)
                list.add(new HadoopSortingTest.FakeSplit(SPLIT_LEN));

            return list;
        }

        /** {@inheritDoc} */
        @Override public RecordReader<IntWritable, Text> createRecordReader(final InputSplit split,
            TaskAttemptContext context) throws IOException, InterruptedException {
            return new RecordReader<IntWritable, Text>() {
                /** */
                int cnt;

                /** */
                Text val = new Text();

                @Override public void initialize(InputSplit split, TaskAttemptContext context) {
                    // No-op.
                }

                @Override public boolean nextKeyValue() throws IOException, InterruptedException {
                    return cnt++ < split.getLength();
                }

                @Override public IntWritable getCurrentKey() {
                    return new IntWritable(cnt);
                }

                @Override public Text getCurrentValue() {
                    UUID id = UUID.randomUUID();

                    assertTrue(HadoopGroupingTestState.values().add(id));

                    val.set(id.toString());

                    return val;
                }

                @Override public float getProgress() {
                    return 0;
                }

                @Override public void close() {
                    // No-op.
                }
            };
        }
    }

    /**
     *
     */
    public static class OutFormat extends OutputFormat {
        /** {@inheritDoc} */
        @Override public RecordWriter getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
            return null;
        }
    }
}