package org.apache.ignite.internal.processors.hadoop.impl;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.examples.terasort.TeraGen;
import org.apache.hadoop.examples.terasort.TeraSort;
import org.apache.hadoop.examples.terasort.TeraValidate;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;

/**
 *
 */
public class HadoopTeraSortExampleTest extends HadoopGenericExampleTest {
    /** Number of data lines. */
    protected final int linesToGenerate = 100_000;

    /**
     * Utility method to add leading zeroes.
     *
     * @param len The desired length.
     * @param n The number to nullify.
     */
    static String nullifyToLen(int len, int n) {
        String res = String.valueOf(n);

        int zero = len - res.length();

        if (zero <= 0)
            return res;

        StringBuilder sb = new StringBuilder();

        for (int i=0; i<zero; i++)
            sb.append("0");

        return sb.toString() + res;
    }

    /** */
    private final GenericHadoopExample teragenEx = new GenericHadoopExample() {
        private final Tool teraGenTool = new TeraGen();

        /** {@inheritDoc} */
        @Override String[] parameters(FrameworkParameters fp) {
            return new String[] {
                String.valueOf(linesToGenerate),
                inDir(fp) };
        }

        /** {@inheritDoc} */
        @Override Tool tool() {
            return teraGenTool;
        }

        /** {@inheritDoc} */
        @Override void verify(String[] parameters) throws Exception {
            int fileIdx = 0;
            int sumLen = 0;

            while (true) {
                Path path = new Path(parameters[1] + "/part-m-" + nullifyToLen(5, fileIdx++));

                if (!getFileSystem().exists(path))
                    break;

                FileStatus status = getFileSystem().getFileStatus(path);

                sumLen += status.getLen();
            }

            assertEquals(linesToGenerate * 100, sumLen);
        }
    };

    /** */
    private final GenericHadoopExample teraSortEx = new GenericHadoopExample() {
        private final Tool teraSortTool = new TeraSort();

        /** {@inheritDoc} */
        @Override String[] parameters(FrameworkParameters fp) {
            return new String[] { inDir(fp), outDir(fp) };
        }

        /** {@inheritDoc} */
        @Override Tool tool() {
            return teraSortTool;
        }

        /** {@inheritDoc} */
        @Override void verify(String[] parameters) throws Exception {
            // noop, because teraValidate example does the verification.
        }
    };

    /** */
    private final GenericHadoopExample teraValidateEx = new GenericHadoopExample() {
        private final Tool teraValidateTool = new TeraValidate();

        private String reportDir(FrameworkParameters fp) {
            return fp.getWorkDir(name()) + "/report";
        }

        /** {@inheritDoc} */
        @Override String[] parameters(FrameworkParameters fp) {
            return new String[] { outDir(fp), reportDir(fp) };
        }

        /** {@inheritDoc} */
        @Override Tool tool() {
            return teraValidateTool;
        }

        /** {@inheritDoc} */
        @Override void verify(String[] parameters) throws Exception {
            Path path = new Path(parameters[1] + "/part-r-00000");

            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(getFileSystem().open(path)))) {
                String line1 = br.readLine();

                assertNotNull(line1);
                assertTrue(line1.length() < 50);
                assertTrue(line1.startsWith("checksum"));

                assertNull(br.readLine());
            }
        }
    };

    /** {@inheritDoc} */
    @Override protected GenericHadoopExample example() {
        return teragenEx;
    }

    /** {@inheritDoc} */
    @Override public void testExample() throws Exception {
        final JobConf conf = new JobConf();

        prepareConf(conf);

        FrameworkParameters fp = frameworkParameters();

        runExample(conf, fp, teragenEx);
        runExample(conf, fp, teraSortEx);
        runExample(conf, fp, teraValidateEx);
    }

    /** {@inheritDoc} */
    @Override protected FrameworkParameters frameworkParameters() {
        return new FrameworkParameters(numMaps(), gridCount(), getFsBase(), getUser()) {
            @Override public String getWorkDir(String exampleName) {
                return workDir + "/" + user + "/tera";
            }
        };
    }
}
