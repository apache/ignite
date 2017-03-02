package org.apache.ignite.internal.processors.hadoop.impl;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.AggregateWordHistogram;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.aggregate.ValueAggregatorJob;
import org.apache.hadoop.util.Tool;
import org.apache.ignite.internal.processors.hadoop.HadoopJobProperty;

/**
 *
 */
public class HadoopAggregateHistogramExampleTest extends HadoopGenericExampleTest {
    /** */
    final Tool tool = new Tool() {
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
        @SuppressWarnings("unchecked")
        @Override public int run(String[] args) throws Exception {
            final Configuration conf = getConf();

            HadoopGenericExampleTest.setAggregatorDescriptors_CORRECT(conf,
                new Class[] { AggregateWordHistogram.AggregateWordHistogramPlugin.class } );

            Job job = ValueAggregatorJob.createValueAggregatorJob(conf, args);

            job.setJarByClass(AggregateWordHistogram.class);

            return job.waitForCompletion(true) ? 0 : 1;
        }
    };

    /** */
    private final GenericHadoopExample ex = new GenericHadoopExample() {
        @Override void prepare(JobConf conf, FrameworkParameters params) throws IOException {
            generateTextInput(11, conf, params);
        }

        /** {@inheritDoc} */
        @Override String[] parameters(FrameworkParameters fp) {
//            System.out.println("usage: inputDirs outDir "
//                + "[numOfReducer [textinputformat|seq [specfile [jobName]]]]");
            return new String[] { inDir(fp), outDir(fp),
                "1", // Numper of reduces other than 1 does not make sense there.
                "textinputformat"
              };
        }

        /** {@inheritDoc} */
        @Override Tool tool() {
            return tool;
        }

        /** {@inheritDoc} */
        @Override void verify(String[] parameters) throws Exception {
            new OutputFileChecker(getFileSystem(), parameters[1] + "/part-r-00000") {
                /** {@inheritDoc} */
                @Override void onFirstLine(String line) {
                    assertTrue(line.startsWith("WORD_HISTOGRAM\t1000\t9\t22\t38\t22.0\t4.78"));
                }

                /** {@inheritDoc} */
                @Override void onFileEnd(int cnt) {
                    assertEquals(1, cnt);
                }
            }.check();
        }
    };

    /** {@inheritDoc} */
    @Override protected void prepareConf(Configuration conf) {
        super.prepareConf(conf);

        // See https://issues.apache.org/jira/browse/IGNITE-4720:
        conf.set(HadoopJobProperty.JOB_SHARED_CLASSLOADER.propertyName(), "false");
    }

    /** {@inheritDoc} */
    @Override protected GenericHadoopExample example() {
        return ex;
    }
}
