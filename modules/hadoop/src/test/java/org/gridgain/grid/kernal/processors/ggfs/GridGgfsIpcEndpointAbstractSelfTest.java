/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.ggfs.hadoop.v1.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.ggfs.GridGgfsConfiguration.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.ggfs.GridGgfsMode.*;

import static org.gridgain.grid.ggfs.hadoop.GridGgfsHadoopParameters.*;
import static org.gridgain.grid.GridSystemProperties.*;

/**
 * Tests for IPC endpoint configuration on both GGFS and FileSystem sides.
 */
public abstract class GridGgfsIpcEndpointAbstractSelfTest extends GridCommonAbstractTest {
    /** Path in FS. */
    private static final Path PATH_FS = new Path("/dir");

    /** Path in GGFS. */
    private static final GridGgfsPath PATH_GGFS = new GridGgfsPath("/dir");

    /** TCP flag. */
    private boolean tcp;

    /** GGFS. */
    private GridGgfsImpl ggfs;

    /** File system. */
    private FileSystem fs;

    /**
     * Constructor.
     *
     * @param tcp TCP flag.
     */
    protected GridGgfsIpcEndpointAbstractSelfTest(boolean tcp) {
        this.tcp = tcp;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ggfs = null;
        fs = null;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        U.closeQuiet(fs);

        stopAllGrids(true);
    }

    /**
     * Start the grid with GGFS.
     *
     * @param gridCnt Grid count.
     * @param endpointEnabled Whether endpoint is enabled.
     * @param endpointCfg Endpoint configuration.
     * @throws Exception If failed.
     */
    private void startUp(int gridCnt, boolean endpointEnabled, @Nullable String endpointCfg) throws Exception {
        for (int i = 0; i < gridCnt; i++) {
            GridGgfsConfiguration ggfsCfg = new GridGgfsConfiguration();

            ggfsCfg.setDataCacheName("partitioned");
            ggfsCfg.setMetaCacheName("replicated");
            ggfsCfg.setName("ggfs");
            ggfsCfg.setBlockSize(512 * 1024);
            ggfsCfg.setDefaultMode(PRIMARY);
            ggfsCfg.setIpcEndpointConfiguration(endpointCfg);
            ggfsCfg.setIpcEndpointEnabled(endpointEnabled);

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

            int startPort = 47500;

            Collection<String> addrs = new ArrayList<>(gridCnt);

            for (int j = 0; j < gridCnt; j++)
                addrs.add(U.getLocalHost().getHostAddress() + ":" + startPort++);

            GridTcpDiscoveryVmIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder();

            ipFinder.setAddresses(addrs);

            GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

            discoSpi.setIpFinder(ipFinder);

            cfg.setDiscoverySpi(discoSpi);
            cfg.setCacheConfiguration(metaCacheCfg, cacheCfg);
            cfg.setGgfsConfiguration(ggfsCfg);
            cfg.setGridName("grid-" + i);

            Grid grid = G.start(cfg);

            if (ggfs == null)
                ggfs = (GridGgfsImpl)grid.ggfs("ggfs");
        }
    }

    /**
     * Test the following use case: - endpoint is enabled; - endpoint configuration is set; - file system endpoint type
     * is set; - file system endpoint port is set.
     *
     * @throws Exception If failed.
     */
    public void testEndpointEnabledEndpointSetTypeSetPortSet() throws Exception {
        check(true, true, true, false, true);
    }

    /**
     * Test the following use case: - endpoint is enabled; - endpoint configuration is set; - file system endpoint type
     * is set; - file system endpoint port is not set.
     *
     * @throws Exception If failed.
     */
    public void testEndpointEnabledEndpointSetTypeSetPortNotSet() throws Exception {
        check(true, true, true, false, false);
    }

    /**
     * Test the following use case: - endpoint is enabled; - endpoint configuration is set; - file system endpoint type
     * is not set; - file system endpoint port is set.
     *
     * @throws Exception If failed.
     */
    public void testEndpointEnabledEndpointSetTypeNotSetPortSet() throws Exception {
        check(true, true, false, false, true);
    }

    /**
     * Test the following use case: - endpoint is enabled; - endpoint configuration is set; - file system endpoint type
     * is not set; - file system endpoint port is not set.
     *
     * @throws Exception If failed.
     */
    public void testEndpointEnabledEndpointSetTypeNotSetPortNotSet() throws Exception {
        check(true, true, false, false, false);
    }

    /**
     * Test the following use case: - endpoint is enabled; - endpoint configuration is not set; - file system endpoint
     * type is set; - file system endpoint port is set.
     *
     * @throws Exception If failed.
     */
    public void testEndpointEnabledEndpointNotSetTypeSetPortSet() throws Exception {
        check(true, false, true, false, true);
    }

    /**
     * Test the following use case: - endpoint is enabled; - endpoint configuration is not set; - file system endpoint
     * type is set; - file system endpoint port is not set.
     *
     * @throws Exception If failed.
     */
    public void testEndpointEnabledEndpointNotSetTypeSetPortNotSet() throws Exception {
        check(true, false, true, false, false);
    }

    /**
     * Test the following use case: - endpoint is enabled; - endpoint configuration is not set; - file system endpoint
     * type is not set; - file system endpoint port is set.
     *
     * @throws Exception If failed.
     */
    public void testEndpointEnabledEndpointNotSetTypeNotSetPortSet() throws Exception {
        check(true, false, false, false, true);
    }

    /**
     * Test the following use case: - endpoint is enabled; - endpoint configuration is not set; - file system endpoint
     * type is not set; - file system endpoint port is not set.
     *
     * @throws Exception If failed.
     */
    public void testEndpointEnabledEndpointNotSetTypeNotSetPortNotSet() throws Exception {
        check(true, false, false, false, false);
    }

    /**
     * Test the following use case: - endpoint is disabled; - endpoint configuration is set.
     *
     * @throws Exception If failed.
     */
    public void testEndpointDisabledEndpointSet() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                check(false, true, true, false, true);

                return null;
            }
        }, IOException.class, null);
    }

    /**
     * Test the following use case: - endpoint is disabled; - endpoint configuration is not set.
     *
     * @throws Exception If failed.
     */
    public void testEndpointDisabledEndpointNotSet() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                check(false, false, true, false, true);

                return null;
            }
        }, IOException.class, null);
    }

    /**
     * Perform main check.
     *
     * @param endpointEnabled Endpoint enabled flag.
     * @param setEndpoint Whether to set explicit endpoint.
     * @param setType Whether to set type.
     * @param setHost Whether IPC host should be set.
     * @param setPort Whether to set port.
     * @throws Exception If failed.
     */
    protected void check(boolean endpointEnabled, boolean setEndpoint, boolean setType, boolean setHost,
        boolean setPort) throws Exception {
        try {
            startUp(2, endpointEnabled, setEndpoint ? endpointConfiguration() : null);

            fs = FileSystem.get(URI.create("ggfs://myGgfs"), fileSystemConfiguration(setType, setHost, setPort));

            fs.mkdirs(PATH_FS);

            ggfs.exists(PATH_GGFS);
        }
        catch (IOException e) {
            throw new IOException("Exception [endpoints=" + ggfs.context().server().endpoints() + ", utilLocHost=" +
                U.getLocalHost().getHostAddress() + ", ggfsLocHost=" +
                ggfs.context().kernalContext().config().getLocalHost() + ", envLocHost=" +
                X.getSystemOrEnv(GG_LOCAL_HOST) + ']', e);
        }
    }

    /**
     * Get IPC endpoint configuration.
     *
     * @return IPC endpoint configuration.
     * @throws Exception If failed.
     */
    private String endpointConfiguration() throws Exception {
        return tcp ? "{type:'tcp', port:" + DFLT_IPC_PORT + ", host:'127.0.0.1'}" :
            "{type:'shmem', port:" + DFLT_IPC_PORT + '}';
    }

    /**
     * Get file system configuration.
     *
     * @param setType Whether IPC type should be set.
     * @param setHost Whether IPC host should be set.
     * @param setPort Whether IPC port should be set.
     * @return Configuration.
     * @throws Exception If failed.
     */
    private Configuration fileSystemConfiguration(boolean setType, boolean setHost, boolean setPort) throws Exception {
        Configuration cfg = new Configuration();

        cfg.set("fs.default.name", "ggfs://myGgfs");
        cfg.set("fs.ggfs.impl", GridGgfsHadoopFileSystem.class.getName());

        if (setType)
            cfg.set(String.format(PARAM_GGFS_ENDPOINT_TYPE, "myGgfs"), tcp ? "tcp" : "shmem");

        if (setHost)
            cfg.set(String.format(PARAM_GGFS_ENDPOINT_HOST, "myGgfs"), "127.0.0.1");

        if (setPort)
            cfg.set(String.format(PARAM_GGFS_ENDPOINT_PORT, "myGgfs"), Integer.toString(DFLT_IPC_PORT));

        return cfg;
    }
}
