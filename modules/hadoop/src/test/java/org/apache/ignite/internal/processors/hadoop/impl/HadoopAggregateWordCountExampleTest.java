package org.apache.ignite.internal.processors.hadoop.impl;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.AggregateWordCount;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.aggregate.ValueAggregatorJob;
import org.apache.hadoop.util.Tool;
import org.apache.ignite.internal.processors.hadoop.HadoopJobProperty;

/**
 *
 */
public class HadoopAggregateWordCountExampleTest extends HadoopGenericExampleTest {
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
            // Original example code looks like the following:
            // ----------------
            //     Job job = ValueAggregatorJob.createValueAggregatorJob(args, new Class[] {WordCountPlugInClass.class});
            //     job.setJarByClass(AggregateWordCount.class);
            //     int ret = job.waitForCompletion(true) ? 0 : 1;
            // ----------------

            final Configuration conf = getConf();

            //setAggregatorDescriptors_WRONG(conf, new Class[] {AggregateWordCount.WordCountPlugInClass.class});
            setAggregatorDescriptors_CORRECT(conf, new Class[] {AggregateWordCount.WordCountPlugInClass.class});

            Job job = ValueAggregatorJob.createValueAggregatorJob(conf, args);

            job.setJarByClass(AggregateWordCount.class);

            return job.waitForCompletion(true) ? 0 : 1;
        }
    };

    /** */
    private final GenericHadoopExample ex = new GenericHadoopExample() {
        @Override void prepare(JobConf conf, FrameworkParameters params) throws IOException {
            generateTextInput(11, conf, params);
        }

        @Override String[] parameters(FrameworkParameters fp) {
            // Parameters: inputDirs outDir
            //  [numOfReducer [textinputformat|seq [specfile [jobName]]]]
            return new String[] {
                inDir(fp),
                outDir(fp),
                "1", // Numper of reduces other than 1 does not make sense.
                "textinputformat"
              };
        }

        @Override Tool tool() {
            return tool;
        }

        @Override void verify(String[] parameters) throws Exception {
            new OutputFileChecker(getFileSystem(), parameters[1] + "/part-r-00000") {
                @Override void onFirstLine(String line) {
                    assertEquals("Aktistetae\t15", line);
                }

                @Override void onLastLine(String line) {
                    assertEquals("zoonitic\t22", line);
                }

                @Override void onFileEnd(int cnt) {
                    assertEquals(1000, cnt);
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
