package org.apache.ignite.internal.processors.hadoop.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.examples.MultiFileWordCount;
import org.apache.hadoop.fs.Path;
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
