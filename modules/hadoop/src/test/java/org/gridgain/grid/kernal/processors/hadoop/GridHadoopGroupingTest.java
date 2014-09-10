/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

import static org.gridgain.grid.kernal.processors.hadoop.GridHadoopUtils.*;

/**
 * Grouping test.
 */
public class GridHadoopGroupingTest extends GridHadoopAbstractSelfTest {
    /** */
    private static final String PATH_OUTPUT = "/test-out";

    /** */
    private static final GridConcurrentHashSet<UUID> vals = new GridConcurrentHashSet<>();

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    protected boolean ggfsEnabled() {
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
    @Override public GridHadoopConfiguration hadoopConfiguration(String gridName) {
        GridHadoopConfiguration cfg = super.hadoopConfiguration(gridName);

        cfg.setExternalExecution(false);

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

        grid(0).hadoop().submit(new GridHadoopJobId(UUID.randomUUID(), 2),
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
        /** {@inheritDoc */
        @Override public int compare(YearTemperature o1, YearTemperature o2) {
            return Integer.compare(o1.year, o2.year);
        }

        /** {@inheritDoc */
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
                list.add(new GridHadoopSortingTest.FakeSplit(20));

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
                    UUID id = UUID.randomUUID();

                    assertTrue(vals.add(id));

                    val.set(id.toString());

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
