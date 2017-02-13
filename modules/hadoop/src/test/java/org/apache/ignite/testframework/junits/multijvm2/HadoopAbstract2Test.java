package org.apache.ignite.testframework.junits.multijvm2;

import java.io.File;
import java.util.List;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.HadoopConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteNodeRunner;
import org.apache.ignite.testframework.junits.multijvm.NodeProcessParameters;

/**
 *
 */
public class HadoopAbstract2Test extends GridCommonAbstractTest {
    /** Initial classpath. */
    private static String initCp;

    /** {@inheritDoc} */
    @Override protected final void beforeTestsStarted() throws Exception {
        // Add surefire classpath to regular classpath.
        initCp = System.getProperty("java.class.path");

        String surefireCp = System.getProperty("surefire.test.class.path");

        if (surefireCp != null)
            System.setProperty("java.class.path", initCp + File.pathSeparatorChar + surefireCp);

        super.beforeTestsStarted();

        beforeTestsStarted0();
    }

    /**
     * Starts the Ignite nodes.
     *
     * @throws Exception On error.
     */
    protected void startNodes() throws Exception {
        for (int idx = 0; idx<gridCount(); idx++) {
            String nodeName = "node-" + idx;

            IgniteConfiguration cfg = getConfiguration(idx, nodeName);

            NodeProcessParameters parameters = getParameters(idx, nodeName);

            new IgniteNodeProxy2(cfg, log(), parameters);
        }

        IgniteNodeProxy2.ensureTopology(gridCount(),
            getConfiguration(gridCount(), "temporaryClientNode"));
    }

    /**
     * Process start parameters for indexed and named node.
     *
     * @return The parameters.
     */
    protected NodeProcessParameters getParameters(int idx, String nodeName) {
        return NodeProcessParameters.DFLT;
    }

    /**
     * Forcibly kills all nodes.
     */
    protected final void killAllNodes() {
        try {
            List<Integer> jvmIds = IgniteNodeRunner.killAll();

            if (!jvmIds.isEmpty())
                log.info("Next processes of IgniteNodeRunner were killed: " + jvmIds);
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Performs additional initialization in the beginning of test class execution.
     *
     * @throws Exception If failed.
     */
    protected void beforeTestsStarted0() throws Exception {
        killAllNodes();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        killAllNodes();

        // Restore classpath.
        System.setProperty("java.class.path", initCp);
    }

    /**
     * Gets configuration for named and indexed node.
     */
    protected IgniteConfiguration getConfiguration(int idx, String gridName) throws Exception {
        IgniteConfiguration cfg = getConfiguration(gridName);

        cfg.setHadoopConfiguration(hadoopConfiguration(idx, gridName));

        if (idx == 0 /*Enable REST only for the 1st node. */) {
            ConnectorConfiguration clnCfg = new ConnectorConfiguration();

            clnCfg.setPort(getMapreduceJobtrackerPort());

            cfg.setConnectorConfiguration(clnCfg);
        }

        cfg.setLocalHost("127.0.0.1");
        cfg.setPeerClassLoadingEnabled(false);

        return cfg;
    }

    /**
     * @return The mapreduce jobtracker port to be used.
     */
    protected int getMapreduceJobtrackerPort() {
        return ConnectorConfiguration.DFLT_TCP_PORT;
    }

    /**
     * @param gridName Grid name.
     * @return Hadoop configuration.
     */
    protected HadoopConfiguration hadoopConfiguration(int idx, String gridName) {
        HadoopConfiguration cfg = new HadoopConfiguration();

        cfg.setMaxParallelTasks(3);

        return cfg;
    }

    /**
     * @return Number of nodes to start.
     */
    protected int gridCount() {
        return 3;
    }
}
