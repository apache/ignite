/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.ggfs;

import org.apache.commons.logging.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.ggfs.hadoop.*;
import org.gridgain.grid.kernal.processors.ggfs.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;

import java.io.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;
import static org.gridgain.grid.util.ipc.shmem.GridIpcSharedMemoryServerEndpoint.*;

/**
 * Test interaction between a GGFS client and a GGFS server.
 */
public class GridGgfsHadoopFileSystemClientSelfTest extends GridGgfsCommonAbstractTest {
    /** Logger. */
    private static final Log LOG = LogFactory.getLog(GridGgfsHadoopFileSystemClientSelfTest.class);

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        G.stopAll(true);
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();
        discoSpi.setIpFinder(new GridTcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(discoSpi);

        GridGgfsConfiguration ggfsCfg = new GridGgfsConfiguration();

        ggfsCfg.setDataCacheName("partitioned");
        ggfsCfg.setMetaCacheName("replicated");
        ggfsCfg.setName("ggfs");
        ggfsCfg.setBlockSize(512 * 1024);
        ggfsCfg.setIpcEndpointConfiguration(GridHadoopTestUtils.jsonToMap("{type:'tcp', port:" + DFLT_IPC_PORT + '}'));

        cfg.setCacheConfiguration(cacheConfiguration());
        cfg.setGgfsConfiguration(ggfsCfg);

        return cfg;
    }

    /**
     * Gets cache configuration.
     *
     * @return Cache configuration.
     */
    protected GridCacheConfiguration[] cacheConfiguration() {
        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName("partitioned");
        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setDistributionMode(PARTITIONED_ONLY);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setEvictionPolicy(null);
        cacheCfg.setAffinityMapper(new GridGgfsGroupDataBlocksKeyMapper(128));
        cacheCfg.setBackups(0);
        cacheCfg.setQueryIndexEnabled(false);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        GridCacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setName("replicated");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        metaCacheCfg.setEvictionPolicy(null);
        metaCacheCfg.setQueryIndexEnabled(false);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        return new GridCacheConfiguration[] {metaCacheCfg, cacheCfg};
    }

    /**
     * Test output stream deferred exception (GG-4440).
     *
     * @throws GridException If failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testOutputStreamDeferredException() throws Exception {
        final byte[] data = "test".getBytes();

        try {
            switchHandlerErrorFlag(true);

            GridGgfsHadoop client = new GridGgfsHadoopOutProc("127.0.0.1", 10500, getTestGridName(0), "ggfs", LOG);

            client.handshake(null);

            GridGgfsPath path = new GridGgfsPath("/test1.file");

            GridGgfsHadoopStreamDelegate delegate = client.create(path, true, false, 1, 1024, null);

            final GridGgfsHadoopOutputStream ggfsOut = new GridGgfsHadoopOutputStream(delegate, LOG,
                GridGgfsHadoopLogger.disabledLogger(), 0);

            // This call should return fine as exception is thrown for the first time.
            ggfsOut.write(data);

            U.sleep(500);

            // This call should throw an IO exception.
            GridTestUtils.assertThrows(null, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    ggfsOut.write(data);

                    return null;
                }
            }, IOException.class, "Failed to write data to server (test).");
        }
        finally {
            switchHandlerErrorFlag(false);
        }
    }

    /**
     * Set GGFS REST handler error flag to the given state.
     *
     * @param flag Flag state.
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    private void switchHandlerErrorFlag(boolean flag) throws Exception {
        GridGgfsProcessorAdapter ggfsProc = ((GridKernal)grid(0)).context().ggfs();

        Map<String, GridGgfsContext> ggfsMap = getField(ggfsProc, "ggfsCache");

        GridGgfsServerManager srvMgr = F.first(ggfsMap.values()).server();

        Collection<GridGgfsServer> srvrs = getField(srvMgr, "srvrs");

        GridGgfsServerHandler ggfsHnd = getField(F.first(srvrs), "hnd");

        Field field = ggfsHnd.getClass().getDeclaredField("errWrite");

        field.setAccessible(true);

        field.set(null, flag);
    }

    /**
     * Get value of the field with the given name of the given object.
     *
     * @param obj Object.
     * @param fieldName Field name.
     * @return Value of the field.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    private <T> T getField(Object obj, String fieldName) throws Exception {
        Field field = obj.getClass().getDeclaredField(fieldName);

        field.setAccessible(true);

        return (T)field.get(obj);
    }
}
