package org.apache.ignite.internal.processors.hadoop.impl;

import java.math.BigDecimal;
import org.apache.hadoop.examples.QuasiMonteCarlo;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;

/**
 * Runs Hadoop Quasi Monte Carlo Pi estimation example.
 */
public class HadoopQuasiMonteCarloTest extends HadoopGenericExampleTest {
    /** Asserted precesion of Pi number estimation. */
    protected static final double PRECISION = 1e-3;

    /** Sort destination dir. */
    protected final String tmpDirStr = getFsBase() + "/pi-tmp";

    /** Estimated Pi value. */
    protected BigDecimal estimatedPi;

    /**
     * Gets base directory.
     * Note that this directory will be completely deleted in the and of the test.
     * @return The base directory.
     */
    protected String getFsBase() {
        return "file:///tmp/" + getUser() + "/hadoop-quasi-monte-carlo-test";
    }

    /**
     * Desired number of maps in TeraSort job.
     * @return The number of maps.
     */
    protected int numMaps() {
        return gridCount() * 16;
    }

    /**
     * Gets the number of points.
     * @return The number of points.
     */
    protected int numPoints() {
        return gridCount() * 100;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        // Delete files used:
        getFileSystem().delete(new Path(getFsBase()), true);
    }

    /** */
    private final GenericHadoopExample ex = new GenericHadoopExample() {
        private final Tool tool = new QuasiMonteCarlo() {
            @Override public int run(String[] args) throws Exception {
                final int nMaps = Integer.parseInt(args[0]);
                final long nSamples = Long.parseLong(args[1]);

                final Path tmpDir = new Path(tmpDirStr);

                System.out.println("Number of Maps  = " + nMaps);
                System.out.println("Samples per Map = " + nSamples);

                estimatedPi = estimatePi(nMaps, nSamples, tmpDir, getConf());

                System.out.println("##### Estimated value of Pi is " + estimatedPi);

                return 0;
            }
        };

        @Override String name() {
            return "QuasiMonteCarlo";
        }

        @Override String[] parameters(FrameworkParameters fp) {
            // "Usage: "+getClass().getName()+" <nMaps> <nSamples>");
            return new String[] {
                String.valueOf(fp.numMaps()),
                String.valueOf(numPoints())
            };
        }

        @Override Tool tool() {
            return tool;
        }

        @Override void verify(String[] parameters) {
            assertEquals(Math.PI, estimatedPi.doubleValue(), PRECISION);
        }
    };

    /** {@inheritDoc} */
    @Override protected GenericHadoopExample example() {
        return ex;
    }
}
