package org.apache.ignite.internal.processors.hadoop.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.WordCount;
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
 *
 */
public class HadoopWordCountExampleTest extends HadoopGenericExampleTest {
    /** */
    final Tool tool = new Tool() {
        private Configuration conf;

        @Override public void setConf(Configuration conf) {
            this.conf = conf;
        }

        @Override public Configuration getConf() {
            return conf;
        }

        @SuppressWarnings("deprecation")
        @Override public int run(String[] args) throws Exception {
            Configuration conf = getConf();

            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

            Job job = new Job(conf, "word count");

            job.setJarByClass(WordCount.class);

            job.setMapperClass(WordCount.TokenizerMapper.class);
            job.setCombinerClass(WordCount.IntSumReducer.class);
            job.setReducerClass(WordCount.IntSumReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

            return job.waitForCompletion(true) ? 0 : 1;
        }
    };

    /** */
    private final GenericHadoopExample ex = new GenericHadoopExample() {

        @Override void prepare(JobConf conf, FrameworkParameters params) throws IOException {
            generateTextInput(1, conf, params);
        }

        @Override String[] parameters(FrameworkParameters fp) {
            return new String[] {
                inDir(fp),
                outDir(fp) };
        }

        @Override Tool tool() {
            return tool;
        }

        @Override void verify(String[] parameters) throws Exception {
            Path path = new Path(parameters[1] + "/part-r-00000");

            try (BufferedReader br = new BufferedReader(
                new InputStreamReader(getFileSystem().open(path)))) {
                int wc = 0;
                String line = null;

                while (true) {
                    String line0 = br.readLine();

                    if (line0 == null)
                        break;

                    line = line0;

                    wc++;

                    if (wc == 1)
                        assertEquals("Alethea\t2", line); // first line
                }

                assertEquals("zoonitic\t3", line); // last line
            }
        }
    };

    /** {@inheritDoc} */
    @Override protected GenericHadoopExample example() {
        return ex;
    }
}
