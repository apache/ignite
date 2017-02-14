package org.apache.ignite.internal.processors.hadoop.impl;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Random;
import org.apache.hadoop.examples.WordMedian;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;

/**
 *
 */
public class HadoopWordMedianExampleTest extends HadoopGenericExampleTest {
    /** */
    private final GenericHadoopExample ex = new GenericHadoopExample() {
        private final WordMedian impl = new WordMedian();

        private final Random random = new Random(0L);

        private String inDir(FrameworkParameters fp) {
            return fp.getWorkDir(name()) + "/in";
        }

        /** {@inheritDoc} */
        @Override void prepare(JobConf conf, FrameworkParameters params) throws IOException {
            // We cannot directly use Hadoop's RandomTextWriter since it is really random, but here
            // we need definitely reproducible input data.
            try (FileSystem fs = FileSystem.get(conf)) {
                try (OutputStream os = fs.create(new Path(inDir(params) + "/in-00"), true)) {
                    HadoopWordMeanExampleTest.generateSentence(random, 2000, os);
                }
            }
        }

        /** {@inheritDoc} */
        @Override String[] parameters(FrameworkParameters fp) {
            // wordmean <in> <out>
            return new String[] {
                inDir(fp),
                fp.getWorkDir(name()) + "/out" };
        }

        /** {@inheritDoc} */
        @Override Tool tool() {
            return impl;
        }

        /** {@inheritDoc} */
        @Override void verify(String[] parameters) {
            assertEquals(9.00, impl.getMedian(), 1e-3);
        }
    };

    /** {@inheritDoc} */
    @Override protected GenericHadoopExample example() {
        return ex;
    }
}
