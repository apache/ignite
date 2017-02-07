package org.apache.ignite.internal.processors.hadoop.impl;

import java.math.BigDecimal;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.QuasiMonteCarlo;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.ignite.configuration.HadoopConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.hadoop.HadoopJobProperty;

/**
 * Runs Hadoop Quasi Monte Carlo Pi estimation example.
 */
public class HadoopQuasiMonteCarloTest extends HadoopAbstractSelfTest {
    /** Asserted precesion of Pi number estimation. */
    protected static final double PRECISION = 1e-3;

    /** Sort destination dir. */
    protected final String tmpDirStr = getFsBase() + "/pi-tmp";

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

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

    /**
     * The user to run Hadoop job on behalf of.
     * @return The user to run Hadoop job on behalf of.
     */
    protected String getUser() {
        return System.getProperty("user.name");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);

        // Delete files used:
        getFileSystem().delete(new Path(getFsBase()), true);
    }

    /** {@inheritDoc} */
    @Override protected final boolean igfsEnabled() {
        // This test uses local file system.
        return false;
    }

    /**
     * Does actual calculation through Ignite API
     *
     * @param gzip Whether to use GZIP.
     */
    protected final void pi(boolean gzip) throws Exception {
        System.out.println("Pi ===============================================================");

        getFileSystem().delete(new Path(tmpDirStr), true);

        final JobConf jobConf = new JobConf();

        jobConf.setUser(getUser());

        jobConf.set("fs.defaultFS", getFsBase());

        log().info("Desired number of maps: " + numMaps());

        jobConf.setBoolean(HadoopJobProperty.SHUFFLE_MAPPER_STRIPED_OUTPUT.propertyName(), true);
        jobConf.setInt(HadoopJobProperty.SHUFFLE_MSG_SIZE.propertyName(), 4096);

        if (gzip)
            jobConf.setBoolean(HadoopJobProperty.SHUFFLE_MSG_GZIP.propertyName(), true);

        jobConf.set(MRConfig.FRAMEWORK_NAME, "ignite" );
        jobConf.set(MRConfig.MASTER_ADDRESS, "localhost:11211");

        // This does real calculation:
        BigDecimal pi = QuasiMonteCarlo.estimatePi(numMaps(), numPoints(), new Path(tmpDirStr), jobConf);

        double deviation = Math.abs(Math.PI - pi.doubleValue());

        System.out.println("Estimated Pi value: " + pi + ", deviation: " + deviation);

        assertTrue(deviation < PRECISION);
    }

    /** {@inheritDoc} */
    @Override protected boolean restEnabled() {
        // Enable mapreduce execution port.
        return true;
    }

    /**
     * Gets the file system we work upon.
     * @return The file system.
     * @throws Exception
     */
    FileSystem getFileSystem() throws Exception{
        return FileSystem.get(new URI(getFsBase()), new Configuration());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrids(gridCount());
    }

    /**
     * Runs Pi estimation.
     *
     * @throws Exception If failed.
     */
    public void testPi() throws Exception {
        checkPi(false);
    }

    /**
     * Check terasort.
     *
     * @param gzip GZIP flag.
     * @throws Exception If failed.
     */
    private void checkPi(boolean gzip) throws Exception {
        pi(gzip);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration igc = super.getConfiguration(gridName);

        HadoopConfiguration hc = createHadoopConfiguration();

        igc.setHadoopConfiguration(hc);

        return igc;
    }

    /**
     * Creates Hadoop configuration for the test.
     * @return The {@link HadoopConfiguration}.
     */
    protected HadoopConfiguration createHadoopConfiguration() {
        HadoopConfiguration hadoopCfg = new HadoopConfiguration();

        // See org.apache.ignite.configuration.HadoopConfiguration.DFLT_MAX_TASK_QUEUE_SIZE
        hadoopCfg.setMaxTaskQueueSize(30_000);

        return hadoopCfg;
    }
}
