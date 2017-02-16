package org.apache.ignite.internal.processors.hadoop.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.AggregateWordHistogram;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.aggregate.ValueAggregatorJob;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

/**
 *
 */
public class HadoopAggregateHistogramExampleTest extends HadoopGenericExampleTest {
    /** */
    final Tool tool = new Tool() {
        private Configuration conf;

        @Override public void setConf(Configuration conf) {
            this.conf = conf;
        }

        @Override public Configuration getConf() {
            return conf;
        }

        @SuppressWarnings("unchecked")
        @Override public int run(String[] args) throws Exception {
            final Configuration conf = getConf();

            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

            HadoopGenericExampleTest.setAggregatorDescriptors(conf,
                new Class[] { AggregateWordHistogram.AggregateWordHistogramPlugin.class } );

            Job job = ValueAggregatorJob.createValueAggregatorJob(conf, otherArgs);

            job.setJarByClass(AggregateWordHistogram.class);

            return job.waitForCompletion(true) ? 0 : 1;
        }
    };

    /** */
    private final GenericHadoopExample ex = new GenericHadoopExample() {
        @Override void prepare(JobConf conf, FrameworkParameters params) throws IOException {
            generateTextInput(11, conf, params);
        }

        @Override String[] parameters(FrameworkParameters fp) {
//            System.out.println("usage: inputDirs outDir "
//                + "[numOfReducer [textinputformat|seq [specfile [jobName]]]]");
            return new String[] {
                inDir(fp),
                outDir(fp),
                "1", // Numper of reduces other than 1 does not make sense there.
                "textinputformat"
              };
        }

        @Override Tool tool() {
            return tool;
        }

        @Override void verify(String[] parameters) throws Exception {
            Path path = new Path(parameters[1] + "/part-r-00000");

            try (BufferedReader br = new BufferedReader(
                new InputStreamReader(getFileSystem().open(path)))) {
                assertTrue(br.readLine().startsWith("WORD_HISTOGRAM\t1000\t9\t22\t38\t22.0\t4.78"));

                assertNull(br.readLine());
            }
        }


    };

    /** {@inheritDoc} */
    @Override protected GenericHadoopExample example() {
        return ex;
    }
}
