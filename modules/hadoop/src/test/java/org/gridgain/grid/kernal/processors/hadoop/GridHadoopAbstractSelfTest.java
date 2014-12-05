/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.apache.hadoop.conf.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.fs.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.processors.hadoop.fs.*;
import org.gridgain.grid.spi.communication.tcp.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Abstract class for Hadoop tests.
 */
public abstract class GridHadoopAbstractSelfTest extends GridCommonAbstractTest {
    /** REST port. */
    protected static final int REST_PORT = 11212;

    /** GGFS name. */
    protected static final String ggfsName = null;

    /** GGFS name. */
    protected static final String ggfsMetaCacheName = "meta";

    /** GGFS name. */
    protected static final String ggfsDataCacheName = "data";

    /** GGFS block size. */
    protected static final int ggfsBlockSize = 1024;

    /** GGFS block group size. */
    protected static final int ggfsBlockGroupSize = 8;

    /** Initial REST port. */
    private int restPort = REST_PORT;

    /** Initial classpath. */
    private static String initCp;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // Add surefire classpath to regular classpath.
        initCp = System.getProperty("java.class.path");

        String surefireCp = System.getProperty("surefire.test.class.path");

        if (surefireCp != null)
            System.setProperty("java.class.path", initCp + File.pathSeparatorChar + surefireCp);

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        // Restore classpath.
        System.setProperty("java.class.path", initCp);

        initCp = null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setHadoopConfiguration(hadoopConfiguration(gridName));

        GridTcpCommunicationSpi commSpi = new GridTcpCommunicationSpi();

        commSpi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(commSpi);

        if (ggfsEnabled()) {
            cfg.setCacheConfiguration(metaCacheConfiguration(), dataCacheConfiguration());

            cfg.setGgfsConfiguration(ggfsConfiguration());
        }

        if (restEnabled()) {
            cfg.setRestEnabled(true);
            cfg.setRestTcpPort(restPort++);
        }

        cfg.setLocalHost("127.0.0.1");
        cfg.setPeerClassLoadingEnabled(false);

        return cfg;
    }

    /**
     * @param gridName Grid name.
     * @return Hadoop configuration.
     */
    public GridHadoopConfiguration hadoopConfiguration(String gridName) {
        GridHadoopConfiguration cfg = new GridHadoopConfiguration();

        cfg.setMaxParallelTasks(3);

        return cfg;
    }

    /**
     * @return GGFS configuration.
     */
    public IgniteFsConfiguration ggfsConfiguration() {
        IgniteFsConfiguration cfg = new IgniteFsConfiguration();

        cfg.setName(ggfsName);
        cfg.setBlockSize(ggfsBlockSize);
        cfg.setDataCacheName(ggfsDataCacheName);
        cfg.setMetaCacheName(ggfsMetaCacheName);
        cfg.setFragmentizerEnabled(false);

        return cfg;
    }

    /**
     * @return GGFS meta cache configuration.
     */
    public GridCacheConfiguration metaCacheConfiguration() {
        GridCacheConfiguration cfg = new GridCacheConfiguration();

        cfg.setName(ggfsMetaCacheName);
        cfg.setCacheMode(REPLICATED);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setWriteSynchronizationMode(FULL_SYNC);

        return cfg;
    }

    /**
     * @return GGFS data cache configuration.
     */
    private GridCacheConfiguration dataCacheConfiguration() {
        GridCacheConfiguration cfg = new GridCacheConfiguration();

        cfg.setName(ggfsDataCacheName);
        cfg.setCacheMode(PARTITIONED);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setAffinityMapper(new IgniteFsGroupDataBlocksKeyMapper(ggfsBlockGroupSize));
        cfg.setWriteSynchronizationMode(FULL_SYNC);

        return cfg;
    }

    /**
     * @return {@code True} if GGFS is enabled on Hadoop nodes.
     */
    protected boolean ggfsEnabled() {
        return false;
    }

    /**
     * @return {@code True} if REST is enabled on Hadoop nodes.
     */
    protected boolean restEnabled() {
        return false;
    }

    /**
     * @return Number of nodes to start.
     */
    protected int gridCount() {
        return 3;
    }

    /**
     * @param cfg Config.
     */
    protected void setupFileSystems(Configuration cfg) {
        cfg.set("fs.defaultFS", ggfsScheme());
        cfg.set("fs.ggfs.impl", org.gridgain.grid.ggfs.hadoop.v1.GridGgfsHadoopFileSystem.class.getName());
        cfg.set("fs.AbstractFileSystem.ggfs.impl", org.gridgain.grid.ggfs.hadoop.v2.GridGgfsHadoopFileSystem.
            class.getName());

        GridHadoopFileSystemsUtils.setupFileSystems(cfg);
    }

    /**
     * @return GGFS scheme for test.
     */
    protected String ggfsScheme() {
        return "ggfs://:" + getTestGridName(0) + "@/";
    }
}
