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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
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
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.GridRandom;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.processors.hadoop.HadoopUtils.createJobInfo;

/**
 * Grouping test.
 */
public class HadoopGroupingTest extends HadoopAbstractSelfTest {
    /** */
    private static final String PATH_OUTPUT = "/test-out";

    /** */
    private static final GridConcurrentHashSet<UUID> vals = HadoopSharedMap.map(HadoopGroupingTest.class)
        .put("vals", new GridConcurrentHashSet<UUID>());

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    protected boolean igfsEnabled() {
        return false;
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
    public void testGroupingReducer() throws Exception {
        doTestGrouping(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testGroupingCombiner() throws Exception {
        doTestGrouping(true);
    }

    /**
     * @param combiner With combiner.
     * @throws Exception If failed.
     */
    public void doTestGrouping(boolean combiner) throws Exception {
        vals.clear();

        Job job = Job.getInstance();

        job.setInputFormatClass(InFormat.class);
        job.setOutputFormatClass(OutFormat.class);

        job.setOutputKeyClass(YearTemperature.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Mapper.class);

        if (combiner) {
            job.setCombinerClass(MyReducer.class);
            job.setNumReduceTasks(0);
            job.setCombinerKeyGroupingComparatorClass(YearComparator.class);
        }
        else {
            job.setReducerClass(MyReducer.class);
            job.setNumReduceTasks(4);
            job.setGroupingComparatorClass(YearComparator.class);
        }

        grid(0).hadoop().submit(new HadoopJobId(UUID.randomUUID(), 2),
            createJobInfo(job.getConfiguration())).get(30000);

        assertTrue(vals.isEmpty());
    }

    public static class MyReducer extends Reducer<YearTemperature, Text, Text, Object> {
        /** */
        int lastYear;

        @Override protected void reduce(YearTemperature key, Iterable<Text> vals0, Context context)
            throws IOException, InterruptedException {
            X.println("___ : " + context.getTaskAttemptID() + " --> " + key);

            Set<UUID> ids = new HashSet<>();

            for (Text val : vals0)
                assertTrue(ids.add(UUID.fromString(val.toString())));

            for (Text val : vals0)
                assertTrue(ids.remove(UUID.fromString(val.toString())));

            assertTrue(ids.isEmpty());

            assertTrue(key.year > lastYear);

            lastYear = key.year;

            for (Text val : vals0)
                assertTrue(vals.remove(UUID.fromString(val.toString())));
        }
    }

    public static class YearComparator implements RawComparator<YearTemperature> { // Grouping comparator.
        /** {@inheritDoc} */
        @Override public int compare(YearTemperature o1, YearTemperature o2) {
            return Integer.compare(o1.year, o2.year);
        }

        /** {@inheritDoc} */
        @Override public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            throw new IllegalStateException();
        }
    }

    public static class YearTemperature implements WritableComparable<YearTemperature>, Cloneable {
        /** */
        private int year;

        /** */
        private int temperature;

        /** {@inheritDoc} */
        @Override public void write(DataOutput out) throws IOException {
            out.writeInt(year);
            out.writeInt(temperature);
        }

        /** {@inheritDoc} */
        @Override public void readFields(DataInput in) throws IOException {
            year = in.readInt();
            temperature = in.readInt();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            throw new IllegalStateException();
        }

        /** {@inheritDoc} */
        @Override public int hashCode() { // To be partitioned by year.
            return year;
        }

        /** {@inheritDoc} */
        @Override public int compareTo(YearTemperature o) {
            int res = Integer.compare(year, o.year);

            if (res != 0)
                return res;

            // Sort comparator by year and temperature, to find max for year.
            return Integer.compare(o.temperature, temperature);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(YearTemperature.class, this);
        }
    }

    public static class InFormat extends InputFormat<YearTemperature, Text> {
        /** {@inheritDoc} */
        @Override public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
            ArrayList<InputSplit> list = new ArrayList<>();

            for (int i = 0; i < 10; i++)
                list.add(new HadoopSortingTest.FakeSplit(20));

            return list;
        }

        /** {@inheritDoc} */
        @Override public RecordReader<YearTemperature, Text> createRecordReader(final InputSplit split,
            TaskAttemptContext context) throws IOException, InterruptedException {
            return new RecordReader<YearTemperature, Text>() {
                /** */
                int cnt;

                /** */
                Random rnd = new GridRandom();

                /** */
                YearTemperature key = new YearTemperature();

                /** */
                Text val = new Text();

                @Override public void initialize(InputSplit split, TaskAttemptContext context) {
                    // No-op.
                }

                @Override public boolean nextKeyValue() throws IOException, InterruptedException {
                    return cnt++ < split.getLength();
                }

                @Override public YearTemperature getCurrentKey() {
                    key.year = 1990 + rnd.nextInt(10);
                    key.temperature = 10 + rnd.nextInt(20);

                    return key;
                }

                @Override public Text getCurrentValue() {
                    UUID id = UUID.randomUUID();

                    assertTrue(vals.add(id));

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