package org.apache.ignite.internal.processors.hadoop.impl;

import java.io.IOException;
import java.lang.reflect.Constructor;
import org.apache.hadoop.examples.Grep;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.ignite.IgniteException;

/**
 *
 */
public class HadoopGrepExampleTest extends HadoopGenericExampleTest {
    /** */
    final Tool tool;

    /**
     * Constructor.
     */
    public HadoopGrepExampleTest() {
        // For some reason Grep() constructor is private:
        try {
            Constructor c = Grep.class.getDeclaredConstructor();

            c.setAccessible(true);

            tool = (Tool)c.newInstance();
        } catch (Exception e) {
            throw new IgniteException(e);
        }
    }

    /** */
    private final GenericHadoopExample ex = new GenericHadoopExample() {
        /** {@inheritDoc} */
        @Override void prepare(JobConf conf, FrameworkParameters params) throws IOException {
            generateTextInput(11, conf, params);
        }

        /** {@inheritDoc} */
        @Override String[] parameters(FrameworkParameters fp) {
            return new String[] {
                inDir(fp),
                outDir(fp),
               "^z[a-z]+" };
        }

        /** {@inheritDoc} */
        @Override Tool tool() {
            return tool;
        }

        /** {@inheritDoc} */
        @Override void verify(String[] parameters) throws Exception {
            new OutputFileChecker(getFileSystem(), parameters[1] + "/part-r-00000") {
                @Override void onFirstLine(String line) {
                    assertEquals("1\tzoonitic", line);
                }

                @Override void onLastLine(String line) {
                    assertEquals("1\tzenick", line);
                }

                @Override void onFileEnd(int cnt) {
                    assertEquals(2, cnt);
                }
            }.check();
        }
    };

    /** {@inheritDoc} */
    @Override protected GenericHadoopExample example() {
        return ex;
    }
}
