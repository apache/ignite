/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.ipc.shmem;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;

import java.util.*;

import static org.gridgain.grid.events.GridEventType.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 *
 */
public class GridIpcSharedMemoryNodeStartup {
    /**
     * @param args Args.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception{
        GridConfiguration cfg = new GridConfiguration();

        GridGgfsConfiguration ggfsCfg = new GridGgfsConfiguration();

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(new GridTcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(discoSpi);

        Map<String, String> endpointCfg = new HashMap<>();

        endpointCfg.put("type", "shmem");
        endpointCfg.put("port", "10500");

        ggfsCfg.setIpcEndpointConfiguration(endpointCfg);

        ggfsCfg.setDataCacheName("partitioned");
        ggfsCfg.setMetaCacheName("partitioned");
        ggfsCfg.setName("ggfs");

        cfg.setGgfsConfiguration(ggfsCfg);

        GridCacheConfiguration cacheCfg = new GridCacheConfiguration();

        cacheCfg.setName("partitioned");
        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setDistributionMode(PARTITIONED_ONLY);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setEvictionPolicy(null);
        cacheCfg.setBackups(0);
        cacheCfg.setQueryIndexEnabled(false);

        cfg.setCacheConfiguration(cacheCfg);

        cfg.setIncludeEventTypes(EVT_TASK_FAILED, EVT_TASK_FINISHED, EVT_JOB_MAPPED);

        try (Grid ignored = G.start(cfg)) {
            X.println("Press any key to stop grid...");

            System.in.read();
        }
    }
}
