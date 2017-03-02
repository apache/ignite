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
        /** */
        private final Tool impl = new MultiFileWordCount();

        /** {@inheritDoc} */
        @Override void prepare(JobConf conf, FrameworkParameters params) throws IOException {
            generateTextInput(11, conf, params);
        }

        /** {@inheritDoc} */
        @Override String[] parameters(FrameworkParameters fp) {
            return new String[] { inDir(fp), outDir(fp) };
        }

        /** {@inheritDoc} */
        @Override Tool tool() {
            return impl;
        }

        /** {@inheritDoc} */
        @Override void verify(String[] parameters) throws Exception {
            new OutputFileChecker(getFileSystem(), parameters[1] + "/part-r-00000") {
                /** {@inheritDoc} */
                @Override void onFirstLine(String line) {
                    assertEquals("Aktistetae\t15", line);
                }

                /** {@inheritDoc} */
                @Override void onLastLine(String line) {
                    assertEquals("zoonitic\t22", line);
                }

                /** {@inheritDoc} */
                @Override void onFileEnd(int cnt) {
                    assertEquals(1000, cnt);
                }
            }.check();
        }
    };

    /** {@inheritDoc} */
    @Override protected GenericHadoopExample example() {
        return ex;
    }
}
