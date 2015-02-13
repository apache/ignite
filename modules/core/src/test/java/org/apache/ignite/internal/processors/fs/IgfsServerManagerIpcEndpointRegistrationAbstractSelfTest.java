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

package org.apache.ignite.internal.processors.fs;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.ignitefs.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.port.*;
import org.apache.ignite.internal.util.ipc.loopback.*;
import org.apache.ignite.internal.util.ipc.shmem.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.configuration.IgniteFsConfiguration.*;

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

        cfg.setGgfsConfiguration(
            igniteFsConfiguration("tcp", DFLT_IPC_PORT, null)
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

        cfg.setGgfsConfiguration(
            igniteFsConfiguration("tcp", DFLT_IPC_PORT, "127.0.0.1"),
            igniteFsConfiguration("tcp", DFLT_IPC_PORT + 1, U.getLocalHost().getHostName()));

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
        cc.setQueryIndexEnabled(false);

        CacheConfiguration metaCfg = defaultCacheConfiguration();

        metaCfg.setName("replicated");
        metaCfg.setCacheMode(CacheMode.REPLICATED);
        metaCfg.setAtomicityMode(TRANSACTIONAL);
        metaCfg.setQueryIndexEnabled(false);

        cfg.setCacheConfiguration(metaCfg, cc);

        return cfg;
    }

    /**
     * Creates test-purposed IgniteFsConfiguration.
     *
     * @param endPntType End point type.
     * @param endPntPort End point port.
     * @param endPntHost End point host.
     * @return test-purposed IgniteFsConfiguration.
     */
    protected IgniteFsConfiguration igniteFsConfiguration(@Nullable String endPntType, @Nullable Integer endPntPort,
        @Nullable String endPntHost) throws IgniteCheckedException {
        HashMap<String, String> endPntCfg = null;

        if (endPntType != null) {
            endPntCfg = new HashMap<>();

            endPntCfg.put("type", endPntType);

            if (endPntPort != null)
                endPntCfg.put("port", String.valueOf(endPntPort));

            if (endPntHost != null)
                endPntCfg.put("host", endPntHost);
        }

        IgniteFsConfiguration ggfsConfiguration = new IgniteFsConfiguration();

        ggfsConfiguration.setDataCacheName("partitioned");
        ggfsConfiguration.setMetaCacheName("replicated");
        ggfsConfiguration.setName("ggfs" + UUID.randomUUID());
        ggfsConfiguration.setManagementPort(mgmtPort.getAndIncrement());

        if (endPntCfg != null)
            ggfsConfiguration.setIpcEndpointConfiguration(endPntCfg);

        return ggfsConfiguration;
    }
}
