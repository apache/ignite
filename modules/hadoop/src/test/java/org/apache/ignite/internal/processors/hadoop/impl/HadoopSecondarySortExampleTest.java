package org.apache.ignite.internal.processors.hadoop.impl;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.SecondarySort;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

/**
 * Hadoop secondary sorting example.
 */
public class HadoopSecondarySortExampleTest extends HadoopGenericExampleTest {
    /** Number of input rows to sort. */
    static final int numberOrRows = 100_000;

    /** Generator to make the input data. */
    static final Random rnd = new Random(0L);

    /** */
    static class SecondarySortTool implements Tool {
        /** */
        private Configuration conf;

        /** {@inheritDoc} */
        @Override public void setConf(Configuration conf) {
            this.conf = conf;
        }

        /** {@inheritDoc} */
        @Override public Configuration getConf() {
            return conf;
        }

        /** {@inheritDoc} */
        @Override public int run(String[] args) throws Exception {
            // SecondarySort does not implement tool
            Configuration conf = getConf();

            assert conf != null;

            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

            if (otherArgs.length != 2) {
                System.err.println("Usage: secondarysort <in> <out>");

                return 2;
            }

            Job job = new Job(conf, "secondary sort");

            job.setJarByClass(SecondarySort.class);
            job.setMapperClass(SecondarySort.MapClass.class);
            job.setReducerClass(SecondarySort.Reduce.class);

            // group and partition by the first int in the pair
            job.setPartitionerClass(SecondarySort.FirstPartitioner.class);
            job.setGroupingComparatorClass(SecondarySort.FirstGroupingComparator.class);

            // the map output is IntPair, IntWritable
            job.setMapOutputKeyClass(SecondarySort.IntPair.class);
            job.setMapOutputValueClass(IntWritable.class);

            // the reduce output is Text, IntWritable
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

            return job.waitForCompletion(true) ? 0 : 1;
        }
    }

    /** */
    private final GenericHadoopExample ex = new GenericHadoopExample() {
        /** */
        private final Tool dbCntPageView = new SecondarySortTool();

        /** */
        private String inDir;

        @Override String[] parameters(FrameworkParameters fp) {
            // No mandatory parameters.
            return new String[] { inDir, fp.getWorkDir(name()) + "/out" };
        }

        /** {@inheritDoc} */
        @Override Tool tool() {
            return dbCntPageView;
        }

        /** {@inheritDoc} */
        @Override void verify(String[] parameters) throws Exception {
            // Read the file and verify the secondary sorting.
            String outDir = parameters[parameters.length - 1];

            boolean newSection = true;

            int first = -1;
            int minY = Integer.MIN_VALUE;

            try (InputStream is = getFileSystem().open(new Path(outDir + "/part-r-00000"));
                   BufferedReader br = new BufferedReader(new InputStreamReader(is))) {
                String line;

                while (true) {
                    line = br.readLine();

                    if (line == null)
                        break;

                    if (line.startsWith("----"))
                        newSection = true; // Section separator.
                    else {
                        String[] tok = line.split("\\s+");

                        assertEquals("Unexpected line, [line=" + line + ']', 2, tok.length);

                        int x = Integer.parseInt(tok[0]);
                        int y = Integer.parseInt(tok[1]);

                        if (newSection) {
                            first = x;
                            minY = y;

                            newSection = false;
                        }
                        else {
                            assertEquals(first, x);

                            assertTrue(y >= minY);
                        }
                    }
                }
            }
        }

        /** {@inheritDoc} */
        @Override void prepare(JobConf conf, FrameworkParameters fp) throws Exception {
            super.prepare(conf, fp);

            inDir = fp.getWorkDir(name()) + "/in";

            getFileSystem().mkdirs(new Path(inDir));

            generateInput(inDir + "/in00");
        }

        private void generateInput(String path) throws Exception {
            try (OutputStream os = getFileSystem().create(new Path(path), true)) {
                for (int i=0; i<numberOrRows; i++) {
                    // Limit the range there to have non-trivial grouping:
                    int first = rnd.nextInt(numberOrRows/2);
                    int second = rnd.nextInt();

                    String line = first + " " + second + "\n";

                    os.write(line.getBytes(Charset.forName("UTF-8")));
                }
            }
        }
    };

    /** {@inheritDoc} */
    @Override protected GenericHadoopExample example() {
        return ex;
    }
}
