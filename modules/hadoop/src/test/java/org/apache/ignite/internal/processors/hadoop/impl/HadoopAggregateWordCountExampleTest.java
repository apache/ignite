package org.apache.ignite.internal.processors.hadoop.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.AggregateWordCount;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.aggregate.ValueAggregatorJob;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

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
            final Configuration conf = getConf();

            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

            HadoopGenericExampleTest.setAggregatorDescriptors(conf,
                new Class[] { AggregateWordCount.WordCountPlugInClass.class } );

            Job job = ValueAggregatorJob.createValueAggregatorJob(conf, otherArgs);

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
//            System.out.println("usage: inputDirs outDir "
//                + "[numOfReducer [textinputformat|seq [specfile [jobName]]]]");
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
                        assertEquals("Aktistetae\t15", line); // first line
                }

                assertEquals("zoonitic\t22", line); // last line
                assertEquals(1000, wc);
            }
        }
    };

    /** {@inheritDoc} */
    @Override protected GenericHadoopExample example() {
        return ex;
    }
}
