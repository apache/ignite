/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Grouping test.
 */
public class GridHadoopGroupingTest extends GridHadoopAbstractSelfTest {
    /** */
    private static final String PATH_INPUT = "/test-in";

    /** */
    private static final String PATH_OUTPUT = "/test-out";

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /**
     * @return {@code True} if GGFS is enabled on Hadoop nodes.
     */
    protected boolean ggfsEnabled() {
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
    @Override public GridHadoopConfiguration hadoopConfiguration(String gridName) {
        GridHadoopConfiguration cfg = super.hadoopConfiguration(gridName);

        cfg.setExternalExecution(false);

        return cfg;
    }

    public void doTestGrouping(boolean combiner) throws Exception {
        Job job = Job.getInstance();

        job.setInputFormatClass(InFormat.class);

        job.setOutputKeyClass(YearTemperature.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Mapper.class);
        job.setNumReduceTasks(4);

        setupFileSytems(job.getConfiguration());

        FileOutputFormat.setOutputPath(job, new Path(ggfsScheme() + PATH_INPUT));


    }

    public static class MyReducer extends Reducer<YearTemperature, Text, Object, Object> {



    }

    public static class YearComparator implements Comparator<YearTemperature> { // Grouping comparator.
        /** {@inheritDoc */
        @Override public int compare(YearTemperature o1, YearTemperature o2) {
            return Integer.compare(o1.year, o2.year);
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
                list.add(new GridHadoopSortingTest.FakeSplit(23));

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

                Text val = new Text();

                @Override public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
                    // No-op.
                }

                @Override public boolean nextKeyValue() throws IOException, InterruptedException {
                    return cnt++ < split.getLength();
                }

                @Override public YearTemperature getCurrentKey() throws IOException, InterruptedException {
                    key.year = 1990 + rnd.nextInt(10);
                    key.temperature = 10 + rnd.nextInt(20);

                    return key;
                }

                @Override public Text getCurrentValue() throws IOException, InterruptedException {
                    val.set(UUID.randomUUID().toString());

                    return val;
                }

                @Override public float getProgress() throws IOException, InterruptedException {
                    return 0;
                }

                @Override public void close() throws IOException {
                    // No-op.
                }
            };
        }
    }
}
