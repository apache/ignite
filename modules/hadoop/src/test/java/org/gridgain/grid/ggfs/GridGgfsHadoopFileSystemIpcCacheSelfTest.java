/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.ggfs;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.ggfs.hadoop.*;
import org.gridgain.grid.kernal.processors.ggfs.*;
import org.gridgain.grid.spi.communication.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.ipc.shmem.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;

import java.lang.reflect.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.apache.ignite.events.IgniteEventType.*;

/**
 * IPC cache test.
 */
public class GridGgfsHadoopFileSystemIpcCacheSelfTest extends GridGgfsCommonAbstractTest {
    /** IP finder. */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** Path to test hadoop configuration. */
    private static final String HADOOP_FS_CFG = "modules/core/src/test/config/hadoop/core-site.xml";

    /** Group size. */
    public static final int GRP_SIZE = 128;

    /** Started grid counter. */
    private static int cnt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();
        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        IgniteFsConfiguration ggfsCfg = new IgniteFsConfiguration();

        ggfsCfg.setDataCacheName("partitioned");
        ggfsCfg.setMetaCacheName("replicated");
        ggfsCfg.setName("ggfs");
        ggfsCfg.setManagementPort(IgniteFsConfiguration.DFLT_MGMT_PORT + cnt);

        ggfsCfg.setIpcEndpointConfiguration(GridGgfsTestUtils.jsonToMap(
            "{type:'shmem', port:" + (GridIpcSharedMemoryServerEndpoint.DFLT_IPC_PORT + cnt) + "}"));

        ggfsCfg.setBlockSize(512 * 1024); // Together with group blocks mapper will yield 64M per node groups.

        cfg.setGgfsConfiguration(ggfsCfg);

        cfg.setCacheConfiguration(cacheConfiguration());

        cfg.setIncludeEventTypes(EVT_TASK_FAILED, EVT_TASK_FINISHED, EVT_JOB_MAPPED);

        GridTcpCommunicationSpi commSpi = new GridTcpCommunicationSpi();

        commSpi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(commSpi);

        cnt++;

        return cfg;
    }

    /**
     * Gets cache configuration.
     *
     * @return Cache configuration.
     */
    private GridCacheConfiguration[] cacheConfiguration() {
        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName("partitioned");
        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setDistributionMode(GridCacheDistributionMode.PARTITIONED_ONLY);
        cacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAffinityMapper(new GridGgfsGroupDataBlocksKeyMapper(GRP_SIZE));
        cacheCfg.setBackups(0);
        cacheCfg.setQueryIndexEnabled(false);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        GridCacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setName("replicated");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setQueryIndexEnabled(false);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        return new GridCacheConfiguration[] {metaCacheCfg, cacheCfg};
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(4);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        G.stopAll(true);
    }

    /**
     * Test how IPC cache map works.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void testIpcCache() throws Exception {
        Field cacheField = GridGgfsHadoopIpcIo.class.getDeclaredField("ipcCache");

        cacheField.setAccessible(true);

        Field activeCntField = GridGgfsHadoopIpcIo.class.getDeclaredField("activeCnt");

        activeCntField.setAccessible(true);

        Map<String, GridGgfsHadoopIpcIo> cache = (Map<String, GridGgfsHadoopIpcIo>)cacheField.get(null);

        String name = "ggfs:" + getTestGridName(0) + "@";

        Configuration cfg = new Configuration();

        cfg.addResource(U.resolveGridGainUrl(HADOOP_FS_CFG));
        cfg.setBoolean("fs.ggfs.impl.disable.cache", true);
        cfg.setBoolean(String.format(GridGgfsHadoopUtils.PARAM_GGFS_ENDPOINT_NO_EMBED, name), true);

        // Ensure that existing IO is reused.
        FileSystem fs1 = FileSystem.get(new URI("ggfs://" + name + "/"), cfg);

        assertEquals(1, cache.size());

        GridGgfsHadoopIpcIo io = null;

        System.out.println("CACHE: " + cache);

        for (String key : cache.keySet()) {
            if (key.contains("10500")) {
                io = cache.get(key);

                break;
            }
        }

        assert io != null;

        assertEquals(1, ((AtomicInteger)activeCntField.get(io)).get());

        // Ensure that when IO is used by multiple file systems and one of them is closed, IO is not stopped.
        FileSystem fs2 = FileSystem.get(new URI("ggfs://" + name + "/abc"), cfg);

        assertEquals(1, cache.size());
        assertEquals(2, ((AtomicInteger)activeCntField.get(io)).get());

        fs2.close();

        assertEquals(1, cache.size());
        assertEquals(1, ((AtomicInteger)activeCntField.get(io)).get());

        Field stopField = GridGgfsHadoopIpcIo.class.getDeclaredField("stopping");

        stopField.setAccessible(true);

        assert !(Boolean)stopField.get(io);

        // Ensure that IO is stopped when nobody else is need it.
        fs1.close();

        assert cache.isEmpty();

        assert (Boolean)stopField.get(io);
    }
}
