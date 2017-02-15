package org.apache.ignite.internal.processors.hadoop.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import org.apache.hadoop.examples.Grep;
import org.apache.hadoop.fs.Path;
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
            // Verify the grep result:
            try (BufferedReader br = new BufferedReader(new InputStreamReader(
                getFileSystem().open(new Path(parameters[1] + "/part-r-00000"))))) {
                assertEquals("1\tzoonitic", br.readLine());
                assertEquals("1\tzenick", br.readLine());

                assertNull(br.readLine()); // EOF
            }
        }
    };

    /** {@inheritDoc} */
    @Override protected GenericHadoopExample example() {
        return ex;
    }
}
