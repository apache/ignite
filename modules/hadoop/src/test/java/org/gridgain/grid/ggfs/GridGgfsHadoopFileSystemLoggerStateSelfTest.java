/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.ggfs;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.ggfs.hadoop.v1.*;
import org.gridgain.grid.kernal.ggfs.common.*;
import org.gridgain.grid.kernal.processors.ggfs.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;

import java.lang.reflect.*;
import java.net.*;
import java.nio.file.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.ggfs.GridGgfsMode.*;
import static org.gridgain.grid.ggfs.hadoop.GridGgfsHadoopParameters.*;

/**
 * Ensures that sampling is really turned on/off.
 */
public class GridGgfsHadoopFileSystemLoggerStateSelfTest extends GridGgfsCommonAbstractTest {
    /** GGFS. */
    private GridGgfsEx ggfs;

    /** File system. */
    private FileSystem fs;

    /** Whether logging is enabled in FS configuration. */
    private boolean logging;

    /** whether sampling is enabled. */
    private Boolean sampling;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        U.closeQuiet(fs);

        ggfs = null;
        fs = null;

        G.stopAll(true);

        logging = false;
        sampling = null;
    }

    /**
     * Startup the grid and instantiate the file system.
     *
     * @throws Exception If failed.
     */
    private void startUp() throws Exception {
        GridGgfsConfiguration ggfsCfg = new GridGgfsConfiguration();

        ggfsCfg.setDataCacheName("partitioned");
        ggfsCfg.setMetaCacheName("replicated");
        ggfsCfg.setName("ggfs");
        ggfsCfg.setBlockSize(512 * 1024);
        ggfsCfg.setDefaultMode(PRIMARY);
        ggfsCfg.setIpcEndpointConfiguration(GridGgfsTestUtils.jsonToMap("{type:'tcp', port:10500}"));

        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName("partitioned");
        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setDistributionMode(GridCacheDistributionMode.PARTITIONED_ONLY);
        cacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAffinityMapper(new GridGgfsGroupDataBlocksKeyMapper(128));
        cacheCfg.setBackups(0);
        cacheCfg.setQueryIndexEnabled(false);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        GridCacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setName("replicated");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setQueryIndexEnabled(false);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        GridConfiguration cfg = new GridConfiguration();

        cfg.setGridName("ggfs-grid");

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(new GridTcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(metaCacheCfg, cacheCfg);
        cfg.setGgfsConfiguration(ggfsCfg);

        cfg.setLocalHost("127.0.0.1");
        cfg.setRestEnabled(false);

        Grid g = G.start(cfg);

        ggfs = (GridGgfsEx)g.ggfs("ggfs");

        ggfs.globalSampling(sampling);

        fs = fileSystem();
    }

    /**
     * When logging is disabled and sampling is not set no-op logger must be used.
     *
     * @throws Exception If failed.
     */
    public void testLoggingDisabledSamplingNotSet() throws Exception {
        startUp();

        assert !logEnabled();
    }

    /**
     * When logging is enabled and sampling is not set file logger must be used.
     *
     * @throws Exception If failed.
     */
    public void testLoggingEnabledSamplingNotSet() throws Exception {
        logging = true;

        startUp();

        assert logEnabled();
    }

    /**
     * When logging is disabled and sampling is disabled no-op logger must be used.
     *
     * @throws Exception If failed.
     */
    public void testLoggingDisabledSamplingDisabled() throws Exception {
        sampling = false;

        startUp();

        assert !logEnabled();
    }

    /**
     * When logging is enabled and sampling is disabled no-op logger must be used.
     *
     * @throws Exception If failed.
     */
    public void testLoggingEnabledSamplingDisabled() throws Exception {
        logging = true;
        sampling = false;

        startUp();

        assert !logEnabled();
    }

    /**
     * When logging is disabled and sampling is enabled file logger must be used.
     *
     * @throws Exception If failed.
     */
    public void testLoggingDisabledSamplingEnabled() throws Exception {
        sampling = true;

        startUp();

        assert logEnabled();
    }

    /**
     * When logging is enabled and sampling is enabled file logger must be used.
     *
     * @throws Exception If failed.
     */
    public void testLoggingEnabledSamplingEnabled() throws Exception {
        logging = true;
        sampling = true;

        startUp();

        assert logEnabled();
    }

    /**
     * Ensure sampling change through API causes changes in logging on subsequent client connections.
     *
     * @throws Exception If failed.
     */
    public void testSamplingChange() throws Exception {
        // Start with sampling not set.
        startUp();

        assert !logEnabled();

        fs.close();

        // "Not set" => true transition.
        ggfs.globalSampling(true);

        fs = fileSystem();

        assert logEnabled();

        fs.close();

        // True => "not set" transition.
        ggfs.globalSampling(null);

        fs = fileSystem();

        assert !logEnabled();

        // "Not-set" => false transition.
        ggfs.globalSampling(false);

        fs = fileSystem();

        assert !logEnabled();

        fs.close();

        // False => "not=set" transition.
        ggfs.globalSampling(null);

        fs = fileSystem();

        assert !logEnabled();

        fs.close();

        // True => false transition.
        ggfs.globalSampling(true);
        ggfs.globalSampling(false);

        fs = fileSystem();

        assert !logEnabled();

        fs.close();

        // False => true transition.
        ggfs.globalSampling(true);

        fs = fileSystem();

        assert logEnabled();
    }

    /**
     * Ensure that log directory is set to GGFS when client FS connects.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    public void testLogDirectory() throws Exception {
        startUp();

        assertEquals(Paths.get(U.getGridGainHome()).normalize().toString(),
            ggfs.clientLogDirectory());
    }

    /**
     * Instantiate new file system.
     *
     * @return New file system.
     * @throws Exception If failed.
     */
    private GridGgfsHadoopFileSystem fileSystem() throws Exception {
        Configuration fsCfg = new Configuration();

        fsCfg.addResource(U.resolveGridGainUrl("modules/core/src/test/config/hadoop/core-site-loopback.xml"));

        fsCfg.setBoolean("fs.ggfs.impl.disable.cache", true);

        if (logging)
            fsCfg.setBoolean(String.format(PARAM_GGFS_LOG_ENABLED, "ggfs:ggfs-grid@"), logging);

        fsCfg.setStrings(String.format(PARAM_GGFS_LOG_DIR, "ggfs:ggfs-grid@"), U.getGridGainHome());

        return (GridGgfsHadoopFileSystem)FileSystem.get(new URI("ggfs://ggfs:ggfs-grid@/"), fsCfg);
    }

    /**
     * Ensure that real logger is used by the file system.
     *
     * @return {@code True} in case path is secondary.
     * @throws Exception If failed.
     */
    private boolean logEnabled() throws Exception {
        assert fs != null;

        Field field = fs.getClass().getDeclaredField("clientLog");

        field.setAccessible(true);

        return ((GridGgfsLogger)field.get(fs)).isLogEnabled();
    }
}
