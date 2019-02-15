/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.igfs;

import java.util.ArrayList;
import java.util.List;
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
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.configuration.FileSystemConfiguration.DFLT_MGMT_PORT;

/**
 * Base test class for {@link IgfsServer} checking IPC endpoint registrations.
 */
@RunWith(JUnit4.class)
public abstract class IgfsServerManagerIpcEndpointRegistrationAbstractSelfTest extends IgfsCommonAbstractTest {
    private static final AtomicInteger mgmtPort = new AtomicInteger(DFLT_MGMT_PORT);

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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
    @Test
    public void testLoopbackEndpointsCustomHostRegistration() throws Exception {
        IgniteConfiguration cfg = gridConfigurationManyIgfsCaches(2);

        cfg.setFileSystemConfiguration(
            igfsConfiguration(IgfsIpcEndpointType.TCP, IgfsIpcEndpointConfiguration.DFLT_PORT, "127.0.0.1",
                "partitioned0", "replicated0"),
            igfsConfiguration(IgfsIpcEndpointType.TCP, IgfsIpcEndpointConfiguration.DFLT_PORT + 1,
                U.getLocalHost().getHostName(), "partitioned1", "replicated1"));

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
     * @throws Exception If failed.
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
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName());

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
     * Creates base grid configuration.
     *
     * @param cacheCtn Caches count.
     *
     * @return Base grid configuration.
     * @throws Exception In case of any error.
     */
    IgniteConfiguration gridConfigurationManyIgfsCaches(int cacheCtn) throws Exception {
        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName());

        List<CacheConfiguration> cachesCfg = new ArrayList<>();

        for (int i = 0; i < cacheCtn; ++i) {
            CacheConfiguration dataCacheCfg = defaultCacheConfiguration();

            dataCacheCfg.setName("partitioned" + i);
            dataCacheCfg.setCacheMode(CacheMode.PARTITIONED);
            dataCacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(128));
            dataCacheCfg.setBackups(0);
            dataCacheCfg.setAtomicityMode(TRANSACTIONAL);

            CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

            metaCacheCfg.setName("replicated" + i);
            metaCacheCfg.setCacheMode(CacheMode.REPLICATED);
            metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

            cachesCfg.add(dataCacheCfg);
            cachesCfg.add(metaCacheCfg);
        }

        cfg.setCacheConfiguration(cachesCfg.toArray(new CacheConfiguration[cachesCfg.size()]));

        return cfg;
    }

    /**
     * Creates test-purposed IgfsConfiguration.
     *
     * @param endPntType End point type.
     * @param endPntPort End point port.
     * @param endPntHost End point host.
     * @return test-purposed IgfsConfiguration.
     * @throws IgniteCheckedException If failed.
     */
    protected FileSystemConfiguration igfsConfiguration(@Nullable IgfsIpcEndpointType endPntType,
        @Nullable Integer endPntPort, @Nullable String endPntHost) throws IgniteCheckedException {

        return igfsConfiguration(endPntType, endPntPort, endPntHost, "partitioned", "replicated");
    }

    /**
     * Creates test-purposed IgfsConfiguration.
     *
     * @param endPntType End point type.
     * @param endPntPort End point port.
     * @param endPntHost End point host.
     * @param dataCacheName Data cache name.
     * @param metaCacheName Meta cache name.
     * @return test-purposed IgfsConfiguration.
     * @throws IgniteCheckedException If failed.
     */
    protected FileSystemConfiguration igfsConfiguration(@Nullable IgfsIpcEndpointType endPntType,
        @Nullable Integer endPntPort, @Nullable String endPntHost, String dataCacheName, String metaCacheName) throws IgniteCheckedException {
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

        igfsConfiguration.setName("igfs" + UUID.randomUUID());
        igfsConfiguration.setManagementPort(mgmtPort.getAndIncrement());

        CacheConfiguration dataCacheCfg = defaultCacheConfiguration();

        dataCacheCfg.setCacheMode(CacheMode.PARTITIONED);
        dataCacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(128));
        dataCacheCfg.setBackups(0);
        dataCacheCfg.setAtomicityMode(TRANSACTIONAL);

        CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setCacheMode(CacheMode.REPLICATED);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        igfsConfiguration.setMetaCacheConfiguration(metaCacheCfg);
        igfsConfiguration.setDataCacheConfiguration(dataCacheCfg);

        if (endPntCfg != null)
            igfsConfiguration.setIpcEndpointConfiguration(endPntCfg);

        return igfsConfiguration;
    }
}
