/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.igfs;

import java.lang.reflect.Field;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.hadoop.igfs.HadoopIgfsIpcIo;
import org.apache.ignite.internal.processors.hadoop.igfs.HadoopIgfsUtils;
import org.apache.ignite.internal.processors.igfs.IgfsCommonAbstractTest;
import org.apache.ignite.internal.util.ipc.shmem.IpcSharedMemoryServerEndpoint;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.events.EventType.EVT_JOB_MAPPED;
import static org.apache.ignite.events.EventType.EVT_TASK_FAILED;
import static org.apache.ignite.events.EventType.EVT_TASK_FINISHED;

/**
 * IPC cache test.
 */
public class IgniteHadoopFileSystemIpcCacheSelfTest extends IgfsCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Path to test hadoop configuration. */
    private static final String HADOOP_FS_CFG = "modules/core/src/test/config/hadoop/core-site.xml";

    /** Group size. */
    public static final int GRP_SIZE = 128;

    /** Started grid counter. */
    private static int cnt;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setDataCacheName("partitioned");
        igfsCfg.setMetaCacheName("replicated");
        igfsCfg.setName("igfs");
        igfsCfg.setManagementPort(FileSystemConfiguration.DFLT_MGMT_PORT + cnt);

        IgfsIpcEndpointConfiguration endpointCfg = new IgfsIpcEndpointConfiguration();

        endpointCfg.setType(IgfsIpcEndpointType.SHMEM);
        endpointCfg.setPort(IpcSharedMemoryServerEndpoint.DFLT_IPC_PORT + cnt);

        igfsCfg.setIpcEndpointConfiguration(endpointCfg);

        igfsCfg.setBlockSize(512 * 1024); // Together with group blocks mapper will yield 64M per node groups.

        cfg.setFileSystemConfiguration(igfsCfg);

        cfg.setCacheConfiguration(cacheConfiguration());

        cfg.setIncludeEventTypes(EVT_TASK_FAILED, EVT_TASK_FINISHED, EVT_JOB_MAPPED);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

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
    private CacheConfiguration[] cacheConfiguration() {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName("partitioned");
        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setNearConfiguration(null);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(GRP_SIZE));
        cacheCfg.setBackups(0);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setName("replicated");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        return new CacheConfiguration[] {metaCacheCfg, cacheCfg};
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
        Field cacheField = HadoopIgfsIpcIo.class.getDeclaredField("ipcCache");

        cacheField.setAccessible(true);

        Field activeCntField = HadoopIgfsIpcIo.class.getDeclaredField("activeCnt");

        activeCntField.setAccessible(true);

        Map<String, HadoopIgfsIpcIo> cache = (Map<String, HadoopIgfsIpcIo>)cacheField.get(null);

        cache.clear(); // avoid influence of previous tests in the same process.

        String name = "igfs:" + getTestGridName(0) + "@";

        Configuration cfg = new Configuration();

        cfg.addResource(U.resolveIgniteUrl(HADOOP_FS_CFG));
        cfg.setBoolean("fs.igfs.impl.disable.cache", true);
        cfg.setBoolean(String.format(HadoopIgfsUtils.PARAM_IGFS_ENDPOINT_NO_EMBED, name), true);

        // Ensure that existing IO is reused.
        FileSystem fs1 = FileSystem.get(new URI("igfs://" + name + "/"), cfg);

        assertEquals(1, cache.size());

        HadoopIgfsIpcIo io = null;

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
        FileSystem fs2 = FileSystem.get(new URI("igfs://" + name + "/abc"), cfg);

        assertEquals(1, cache.size());
        assertEquals(2, ((AtomicInteger)activeCntField.get(io)).get());

        fs2.close();

        assertEquals(1, cache.size());
        assertEquals(1, ((AtomicInteger)activeCntField.get(io)).get());

        Field stopField = HadoopIgfsIpcIo.class.getDeclaredField("stopping");

        stopField.setAccessible(true);

        assert !(Boolean)stopField.get(io);

        // Ensure that IO is stopped when nobody else is need it.
        fs1.close();

        assert cache.isEmpty();

        assert (Boolean)stopField.get(io);
    }
}