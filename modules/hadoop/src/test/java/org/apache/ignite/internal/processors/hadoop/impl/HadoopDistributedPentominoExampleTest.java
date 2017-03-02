package org.apache.ignite.internal.processors.hadoop.impl;

import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.dancing.DistributedPentomino;
import org.apache.hadoop.examples.dancing.Pentomino;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.Tool;

/**
 * Pentomino example in form of test.
 */
public class HadoopDistributedPentominoExampleTest extends HadoopGenericExampleTest {
    /**
     * @return The with.
     */
    protected int width() {
        return 6;
    }

    /**
     * @return The height.
     */
    protected int height() {
        return 10;
    }

    /**
     * @return The expected solution count.
     */
    protected int expectedSolutionCount() {
        return 2339;
    }

    /**
     * @return The pentomino class.
     */
    protected Class<?> pentominoClass() {
        return Pentomino.class;
    }

    /** */
    private final GenericHadoopExample ex = new GenericHadoopExample() {
        /** */
        private final DistributedPentomino impl = new DistributedPentomino();

        /** {@inheritDoc} */
        @Override String[] parameters(FrameworkParameters fp) {
            return new String[] {
                outDir(fp),
                "-depth", "3",
                "-width", String.valueOf(width()),
                "-height", String.valueOf(height()),
            };
        }

        /** {@inheritDoc} */
        @Override Tool tool() {
            return impl;
        }

        /** {@inheritDoc} */
        @Override void verify(String[] parameters) throws Exception {
            new OutputFileChecker(getFileSystem(), parameters[0] + "/part-r-00000") {
                /** */
                private final Pattern ptrn = Pattern.compile("^[0-9]+,[0-9]+,[0-9]+");

                /** */
                private int solutionCnt;

                /** {@inheritDoc} */
                @Override void onLine(String line, int cnt) {
                    if (ptrn.matcher(line).find())
                        solutionCnt++;
                }

                /** {@inheritDoc} */
                @Override void onFileEnd(int cnt) {
                    assertEquals(expectedSolutionCount(), solutionCnt);
                }
            }.check();
        }
    };

    /** {@inheritDoc} */
    @Override protected void prepareConf(Configuration conf) {
        super.prepareConf(conf);

        conf.set(Pentomino.CLASS, pentominoClass().getName());
        conf.set(MRJobConfig.NUM_MAPS, String.valueOf(numMaps()));
    }

    /** {@inheritDoc} */
    @Override protected GenericHadoopExample example() {
        return ex;
    }
}
