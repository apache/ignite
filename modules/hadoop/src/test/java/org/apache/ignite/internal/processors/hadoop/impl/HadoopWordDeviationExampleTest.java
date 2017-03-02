package org.apache.ignite.internal.processors.hadoop.impl;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Random;
import org.apache.hadoop.examples.WordStandardDeviation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;

/**
 *
 */
public class HadoopWordDeviationExampleTest extends HadoopGenericExampleTest {
    /** */
    private final GenericHadoopExample ex = new GenericHadoopExample() {
        private final WordStandardDeviation impl = new WordStandardDeviation();

        @Override void prepare(JobConf conf, FrameworkParameters params) throws IOException {
            generateTextInput(1, conf, params);
        }

        @Override String[] parameters(FrameworkParameters fp) {
            return new String[] { inDir(fp), outDir(fp) };
        }

        @Override Tool tool() {
            return impl;
        }

        @Override void verify(String[] parameters) {
            assertEquals(3.061, impl.getStandardDeviation(), 1e-3);
        }
    };

    /** {@inheritDoc} */
    @Override protected GenericHadoopExample example() {
        return ex;
    }
}
