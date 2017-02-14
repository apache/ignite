package org.apache.ignite.internal.processors.hadoop.impl;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.WordCount;
import org.apache.hadoop.fs.FileSystem;
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

        @Override public int run(String[] args) throws Exception {
            Configuration conf = getConf();

            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

            if (otherArgs.length != 2) {
                System.err.println("Usage: wordcount <in> <out>");

                return 2;
            }

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
        private final Random random = new Random(0L);

        private String inDir(FrameworkParameters fp) {
            return fp.getWorkDir(name()) + "/in";
        }

        @Override void prepare(JobConf conf, FrameworkParameters params) throws IOException {
            // We cannot directly use Hadoop's RandomTextWriter since it is really random, but here
            // we need definitely reproducible input data.
            try (FileSystem fs = FileSystem.get(conf)) {
                try (OutputStream os = fs.create(new Path(inDir(params) + "/in-00"), true)) {
                    HadoopWordMeanExampleTest.generateSentence(random, 2000, os);
                }
            }
        }

        @Override String[] parameters(FrameworkParameters fp) {
            return new String[] {
                inDir(fp),
                fp.getWorkDir(name()) + "/out" };
        }

        @Override Tool tool() {
            return tool;
        }

        @Override void verify(String[] parameters) {
            // TODO: verify the result.
        }
    };

    /** {@inheritDoc} */
    @Override protected GenericHadoopExample example() {
        return ex;
    }
}
