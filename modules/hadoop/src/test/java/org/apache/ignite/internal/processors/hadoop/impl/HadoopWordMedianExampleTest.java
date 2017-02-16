package org.apache.ignite.internal.processors.hadoop.impl;

import java.io.IOException;
import org.apache.hadoop.examples.WordMedian;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;

/**
 *
 */
public class HadoopWordMedianExampleTest extends HadoopGenericExampleTest {
    /** */
    private final GenericHadoopExample ex = new GenericHadoopExample() {
        private final WordMedian impl = new WordMedian();

        /** {@inheritDoc} */
        @Override void prepare(JobConf conf, FrameworkParameters params) throws IOException {
            generateTextInput(1, conf, params);
        }

        /** {@inheritDoc} */
        @Override String[] parameters(FrameworkParameters fp) {
            // wordmean <in> <out>
            return new String[] {
                inDir(fp),
                outDir(fp) };
        }

        /** {@inheritDoc} */
        @Override Tool tool() {
            return impl;
        }

        /** {@inheritDoc} */
        @Override void verify(String[] parameters) {
            assertEquals(10.00, impl.getMedian(), 1e-3);
        }
    };

    /** {@inheritDoc} */
    @Override protected GenericHadoopExample example() {
        return ex;
    }
}
