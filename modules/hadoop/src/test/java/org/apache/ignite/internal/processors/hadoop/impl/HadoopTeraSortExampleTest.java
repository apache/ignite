package org.apache.ignite.internal.processors.hadoop.impl;

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

    /** */
    private final GenericHadoopExample teragenEx = new GenericHadoopExample() {
        /** */
        private final Tool teraGenTool = new TeraGen();

        /** {@inheritDoc} */
        @Override String[] parameters(FrameworkParameters fp) {
            return new String[] { String.valueOf(linesToGenerate), inDir(fp) };
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
        /** */
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
        /** */
        private final Tool teraValidateTool = new TeraValidate();

        /**
         * @param fp The parameters.
         * @return The report dir path.
         */
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
            new OutputFileChecker(getFileSystem(), parameters[1] + "/part-r-00000") {
                @Override void onFirstLine(String line) {
                    assertTrue(line.length() < 50);
                    assertTrue(line.startsWith("checksum"));
                }

                @Override void onFileEnd(int cnt) {
                    assertEquals(1, cnt);
                }
            }.check();
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
                return  getFsBase() + "/" + getUser() + "/tera";
            }
        };
    }
}
