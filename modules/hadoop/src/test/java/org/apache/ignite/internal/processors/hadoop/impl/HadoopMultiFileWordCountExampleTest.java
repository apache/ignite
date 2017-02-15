package org.apache.ignite.internal.processors.hadoop.impl;

import java.io.IOException;
import org.apache.hadoop.examples.MultiFileWordCount;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;

/**
 * Bbp Pi digits example.
 */
public class HadoopMultiFileWordCountExampleTest extends HadoopGenericExampleTest {
    /** {@inheritDoc} */
    protected int numMaps() {
        return gridCount() * 2;
    }

    /** */
    private final GenericHadoopExample ex = new GenericHadoopExample() {
        private final Tool impl = new MultiFileWordCount();

        @Override void prepare(JobConf conf, FrameworkParameters params) throws IOException {
            generateTextInput(11, conf, params);
        }

        @Override String[] parameters(FrameworkParameters fp) {
            return new String[] {
                inDir(fp),
                outDir(fp) };
        }

        @Override Tool tool() {
            return impl;
        }

        @Override void verify(String[] parameters) {
            // TODO: implement
        }
    };

    /** {@inheritDoc} */
    @Override protected GenericHadoopExample example() {
        return ex;
    }
}
