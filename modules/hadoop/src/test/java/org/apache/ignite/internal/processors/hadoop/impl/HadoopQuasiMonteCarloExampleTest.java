package org.apache.ignite.internal.processors.hadoop.impl;

import java.math.BigDecimal;
import org.apache.hadoop.examples.QuasiMonteCarlo;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.ignite.internal.util.typedef.X;

/**
 * Runs Hadoop Quasi Monte Carlo Pi estimation example.
 */
public class HadoopQuasiMonteCarloExampleTest extends HadoopGenericExampleTest {
    /** Asserted precesion of Pi number estimation. */
    protected static final double PRECISION = 1e-3;

    /** Estimated Pi. */
    private BigDecimal estimatedPi;

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

    /** */
    private final GenericHadoopExample ex = new GenericHadoopExample() {
        /** */
        private final Tool tool = new QuasiMonteCarlo() {
            @Override public int run(String[] args) throws Exception {
                final int nMaps = Integer.parseInt(args[0]);
                final long nSamples = Long.parseLong(args[1]);

                final Path tmpDir = new Path(getFsBase() + "/quasi-monte-carlo");

                X.println("Number of Maps  = " + nMaps);
                X.println("Samples per Map = " + nSamples);

                estimatedPi = estimatePi(nMaps, nSamples, tmpDir, getConf());

                X.println("Estimated value of Pi is " + estimatedPi);

                return 0;
            }
        };

        /** {@inheritDoc} */
        @Override String[] parameters(FrameworkParameters fp) {
            return new String[] { String.valueOf(fp.numMaps()), String.valueOf(numPoints()) };
        }

        /** {@inheritDoc} */
        @Override Tool tool() {
            return tool;
        }

        /** {@inheritDoc} */
        @Override void verify(String[] parameters) {
            assertEquals(Math.PI, estimatedPi.doubleValue(), PRECISION);
        }
    };

    /** {@inheritDoc} */
    @Override protected GenericHadoopExample example() {
        return ex;
    }
}
