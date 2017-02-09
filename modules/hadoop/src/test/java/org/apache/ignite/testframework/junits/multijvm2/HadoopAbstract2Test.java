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
        //HadoopFileSystemsUtils.clearFileSystemCache();

        // Add surefire classpath to regular classpath.
        initCp = System.getProperty("java.class.path");

        String surefireCp = System.getProperty("surefire.test.class.path");

        if (surefireCp != null)
            System.setProperty("java.class.path", initCp + File.pathSeparatorChar + surefireCp);

        super.beforeTestsStarted();

        beforeTestsStarted0();
    }

    /**
     *
     * @throws Exception
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
     *
     * @return
     */
    protected NodeProcessParameters getParameters(int idx, String nodeName) {
        return NodeProcessParameters.DFLT;
    }

    /**
     * Forcibly kills all nodes ran
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
     * @throws Exception If failed.
     */
    protected void beforeTestsStarted0() throws Exception {
        killAllNodes();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        // Restore classpath.
        System.setProperty("java.class.path", initCp);

        initCp = null;

        killAllNodes();
    }

    /** {@inheritDoc} */
    protected IgniteConfiguration getConfiguration(int idx, String gridName) throws Exception {
        IgniteConfiguration cfg = getConfiguration(gridName);

        cfg.setHadoopConfiguration(hadoopConfiguration(idx, gridName));

//        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();
//
//        commSpi.setSharedMemoryPort(-1);
//
//        cfg.setCommunicationSpi(commSpi);

//        TcpDiscoverySpi discoSpi = (TcpDiscoverySpi)cfg.getDiscoverySpi();
//
//        discoSpi.setIpFinder(IP_FINDER);

//        if (igfsEnabled()) {
//            cfg.setCacheConfiguration(metaCacheConfiguration(), dataCacheConfiguration());
//
//            cfg.setFileSystemConfiguration(igfsConfiguration());
//        }

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

//    /**
//     * @return IGFS configuration.
//     */
//    public FileSystemConfiguration igfsConfiguration() throws Exception {
//        FileSystemConfiguration cfg = new FileSystemConfiguration();
//
//        cfg.setName(igfsName);
//        cfg.setBlockSize(igfsBlockSize);
//        cfg.setDataCacheName(igfsDataCacheName);
//        cfg.setMetaCacheName(igfsMetaCacheName);
//        cfg.setFragmentizerEnabled(false);
//
//        return cfg;
//    }

//    /**
//     * @return IGFS meta cache configuration.
//     */
//    public CacheConfiguration metaCacheConfiguration() {
//        CacheConfiguration cfg = new CacheConfiguration();
//
//        cfg.setName(igfsMetaCacheName);
//        cfg.setCacheMode(REPLICATED);
//        cfg.setAtomicityMode(TRANSACTIONAL);
//        cfg.setWriteSynchronizationMode(FULL_SYNC);
//
//        return cfg;
//    }

//    /**
//     * @return IGFS data cache configuration.
//     */
//    protected CacheConfiguration dataCacheConfiguration() {
//        CacheConfiguration cfg = new CacheConfiguration();
//
//        cfg.setName(igfsDataCacheName);
//        cfg.setCacheMode(PARTITIONED);
//        cfg.setAtomicityMode(TRANSACTIONAL);
//        cfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(igfsBlockGroupSize));
//        cfg.setWriteSynchronizationMode(FULL_SYNC);
//
//        return cfg;
//    }

//    /**
//     * @return {@code True} if IGFS is enabled on Hadoop nodes.
//     */
//    protected boolean igfsEnabled() {
//        return false;
//    }

//    /**
//     * @return {@code True} if REST is enabled on Hadoop nodes.
//     */
//    protected boolean restEnabled() {
//        return false;
//    }

    /**
     * @return Number of nodes to start.
     */
    protected int gridCount() {
        return 3;
    }

//    /**
//     * @param cfg Config.
//     */
//    protected void setupFileSystems(Configuration cfg) {
//        cfg.set("fs.defaultFS", igfsScheme());
//        cfg.set("fs.igfs.impl", org.apache.ignite.hadoop.fs.v1.IgniteHadoopFileSystem.class.getName());
//        cfg.set("fs.AbstractFileSystem.igfs.impl", IgniteHadoopFileSystem.
//            class.getName());
//
//        //HadoopFileSystemsUtils.setupFileSystems(cfg);
//    }

//    /**
//     * @return IGFS scheme for test.
//     */
//    protected String igfsScheme() {
//        return "igfs://@/";
//    }
}
