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

package org.apache.ignite.internal.processors.igfs;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.igfs.IgfsIpcEndpointConfiguration;
import org.apache.ignite.igfs.IgfsIpcEndpointType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.port.GridPortRecord;
import org.apache.ignite.internal.util.ipc.loopback.IpcServerTcpEndpoint;
import org.apache.ignite.internal.util.ipc.shmem.IpcSharedMemoryServerEndpoint;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.configuration.FileSystemConfiguration.DFLT_MGMT_PORT;

/**
 * Base test class for {@link IgfsServer} checking IPC endpoint registrations.
 */
public abstract class IgfsServerManagerIpcEndpointRegistrationAbstractSelfTest extends IgfsCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    private static final AtomicInteger mgmtPort = new AtomicInteger(DFLT_MGMT_PORT);

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoopbackEndpointsRegistration() throws Exception {
        IgniteConfiguration cfg = gridConfiguration();

        cfg.setFileSystemConfiguration(
            igfsConfiguration(IgfsIpcEndpointType.TCP, IgfsIpcEndpointConfiguration.DFLT_PORT, null)
        );

        G.start(cfg);

        T2<Integer, Integer> res = checkRegisteredIpcEndpoints();

        // One regular enpoint + one management endpoint.
        assertEquals(2, res.get1().intValue());
        assertEquals(0, res.get2().intValue());
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoopbackEndpointsCustomHostRegistration() throws Exception {
        IgniteConfiguration cfg = gridConfiguration();

        cfg.setFileSystemConfiguration(
            igfsConfiguration(IgfsIpcEndpointType.TCP, IgfsIpcEndpointConfiguration.DFLT_PORT, "127.0.0.1"),
            igfsConfiguration(IgfsIpcEndpointType.TCP, IgfsIpcEndpointConfiguration.DFLT_PORT + 1,
                U.getLocalHost().getHostName()));

        G.start(cfg);

        T2<Integer, Integer> res = checkRegisteredIpcEndpoints();

        // Two regular endpoints + two management endpoints.
        assertEquals(4, res.get1().intValue());
        assertEquals(0, res.get2().intValue());
    }

    /**
     * Counts all registered IPC endpoints.
     *
     * @return Tuple2 where (tcp endpoints count, shmem endpoints count).
     */
    protected T2<Integer, Integer> checkRegisteredIpcEndpoints() throws Exception {
        GridKernalContext ctx = ((IgniteKernal)grid()).context();

        int tcp = 0;
        int shmem = 0;

        for (GridPortRecord record : ctx.ports().records()) {
            if (record.clazz() == IpcSharedMemoryServerEndpoint.class)
                shmem++;
            else if (record.clazz() == IpcServerTcpEndpoint.class)
                tcp++;
        }

        return new T2<>(tcp, shmem);
    }

    /**
     * Creates base grid configuration.
     *
     * @return Base grid configuration.
     * @throws Exception In case of any error.
     */
    protected IgniteConfiguration gridConfiguration() throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestGridName());

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setName("partitioned");
        cc.setCacheMode(CacheMode.PARTITIONED);
        cc.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(128));
        cc.setBackups(0);
        cc.setAtomicityMode(TRANSACTIONAL);

        CacheConfiguration metaCfg = defaultCacheConfiguration();

        metaCfg.setName("replicated");
        metaCfg.setCacheMode(CacheMode.REPLICATED);
        metaCfg.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheConfiguration(metaCfg, cc);

        return cfg;
    }

    /**
     * Creates test-purposed IgfsConfiguration.
     *
     * @param endPntType End point type.
     * @param endPntPort End point port.
     * @param endPntHost End point host.
     * @return test-purposed IgfsConfiguration.
     */
    protected FileSystemConfiguration igfsConfiguration(@Nullable IgfsIpcEndpointType endPntType,
        @Nullable Integer endPntPort, @Nullable String endPntHost) throws IgniteCheckedException {
        IgfsIpcEndpointConfiguration endPntCfg = null;

        if (endPntType != null) {
            endPntCfg = new IgfsIpcEndpointConfiguration();

            endPntCfg.setType(endPntType);

            if (endPntPort != null)
                endPntCfg.setPort(endPntPort);

            if (endPntHost != null)
                endPntCfg.setHost(endPntHost);
        }

        FileSystemConfiguration igfsConfiguration = new FileSystemConfiguration();

        igfsConfiguration.setDataCacheName("partitioned");
        igfsConfiguration.setMetaCacheName("replicated");
        igfsConfiguration.setName("igfs" + UUID.randomUUID());
        igfsConfiguration.setManagementPort(mgmtPort.getAndIncrement());

        if (endPntCfg != null)
            igfsConfiguration.setIpcEndpointConfiguration(endPntCfg);

        return igfsConfiguration;
    }
}