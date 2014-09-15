/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.ggfs.filesystem;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.marshaller.optimized.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Starts up an empty node with GGFS configuration with configured endpoint.
 * <p>
 * The difference is that running this class from IDE adds all example classes to classpath
 * but running from command line doesn't.
 */
public class GgfsFileSystemNodeStartup {
    /**
     * Start up an empty node with specified cache configuration.
     *
     * @param args Command line arguments, none required.
     * @throws GridException If example execution failed.
     */
    public static void main(String[] args) throws GridException {
        GridGain.start(configuration());
    }

    /**
     * Create Grid configuration with GGFS and enabled IPC.
     *
     * @return Grid configuration.
     * @throws GridException If configuration creation failed.
     */
    public static GridConfiguration configuration() throws GridException {
        GridConfiguration cfg = new GridConfiguration();

        cfg.setLocalHost("127.0.0.1");

        GridOptimizedMarshaller marsh = new GridOptimizedMarshaller();

        marsh.setRequireSerializable(false);

        cfg.setMarshaller(marsh);

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        GridTcpDiscoveryVmIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder();

        ipFinder.setAddresses(Collections.singletonList("127.0.0.1:47500..47509"));

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        GridCacheConfiguration metaCacheCfg = new GridCacheConfiguration();

        metaCacheCfg.setName("ggfs-meta");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);
        metaCacheCfg.setQueryIndexEnabled(false);
        metaCacheCfg.setWriteSynchronizationMode(FULL_SYNC);

        GridCacheConfiguration dataCacheCfg = new GridCacheConfiguration();

        dataCacheCfg.setName("ggfs-data");
        dataCacheCfg.setCacheMode(PARTITIONED);
        dataCacheCfg.setAtomicityMode(TRANSACTIONAL);
        dataCacheCfg.setQueryIndexEnabled(false);
        dataCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        dataCacheCfg.setDistributionMode(PARTITIONED_ONLY);
        dataCacheCfg.setBackups(0);
        dataCacheCfg.setAffinityMapper(new GridGgfsGroupDataBlocksKeyMapper(512));

        cfg.setCacheConfiguration(metaCacheCfg, dataCacheCfg);

        GridGgfsConfiguration ggfsCfg = new GridGgfsConfiguration();

        ggfsCfg.setName("ggfs");
        ggfsCfg.setMetaCacheName("ggfs-meta");
        ggfsCfg.setDataCacheName("ggfs-data");
        ggfsCfg.setBlockSize(128 * 1024);
        ggfsCfg.setPerNodeBatchSize(512);
        ggfsCfg.setPerNodeParallelBatchCount(16);
        ggfsCfg.setPrefetchBlocks(32);

        Map<String, String> endpointCfg = new HashMap<>();

        if (GridUtils.isWindows())
            endpointCfg.put("type", "tcp");
        else {
            endpointCfg.put("type", "shmem");
            endpointCfg.put("port", "10500");
        }

        ggfsCfg.setIpcEndpointConfiguration(endpointCfg);

        cfg.setGgfsConfiguration(ggfsCfg);

        return cfg;
    }
}
