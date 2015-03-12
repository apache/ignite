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

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.hadoop.fs.v2.*;
import org.apache.ignite.internal.processors.igfs.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;

import java.io.*;
import java.net.*;
import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;
import static org.apache.ignite.igfs.IgfsMode.*;
import static org.apache.ignite.internal.processors.hadoop.igfs.HadoopIgfsUtils.*;
import static org.apache.ignite.internal.util.ipc.shmem.IpcSharedMemoryServerEndpoint.*;

/**
 * Tests for IGFS file system handshake.
 */
public class IgniteHadoopFileSystemHandshakeSelfTest extends IgfsCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Grid name. */
    private static final String GRID_NAME = "grid";

    /** IGFS name. */
    private static final String IGFS_NAME = "igfs";

    /** IGFS path. */
    private static final IgfsPath PATH = new IgfsPath("/path");

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);
    }

    /**
     * Tests for Grid and IGFS having normal names.
     *
     * @throws Exception If failed.
     */
    public void testHandshake() throws Exception {
        startUp(false, false);

        checkValid(IGFS_NAME + ":" + GRID_NAME + "@");
        checkValid(IGFS_NAME + ":" + GRID_NAME + "@127.0.0.1");
        checkValid(IGFS_NAME + ":" + GRID_NAME + "@127.0.0.1:" + DFLT_IPC_PORT);

        checkInvalid(IGFS_NAME + "@");
        checkInvalid(IGFS_NAME + "@127.0.0.1");
        checkInvalid(IGFS_NAME + "@127.0.0.1:" + DFLT_IPC_PORT);

        checkInvalid(":" + GRID_NAME + "@");
        checkInvalid(":" + GRID_NAME + "@127.0.0.1");
        checkInvalid(":" + GRID_NAME + "@127.0.0.1:" + DFLT_IPC_PORT);

        checkInvalid("");
        checkInvalid("127.0.0.1");
        checkInvalid("127.0.0.1:" + DFLT_IPC_PORT);
    }

    /**
     * Tests for Grid having {@code null} name and IGFS having normal name.
     *
     * @throws Exception If failed.
     */
    public void testHandshakeDefaultGrid() throws Exception {
        startUp(true, false);

        checkInvalid(IGFS_NAME + ":" + GRID_NAME + "@");
        checkInvalid(IGFS_NAME + ":" + GRID_NAME + "@127.0.0.1");
        checkInvalid(IGFS_NAME + ":" + GRID_NAME + "@127.0.0.1:" + DFLT_IPC_PORT);

        checkValid(IGFS_NAME + "@");
        checkValid(IGFS_NAME + "@127.0.0.1");
        checkValid(IGFS_NAME + "@127.0.0.1:" + DFLT_IPC_PORT);

        checkInvalid(":" + GRID_NAME + "@");
        checkInvalid(":" + GRID_NAME + "@127.0.0.1");
        checkInvalid(":" + GRID_NAME + "@127.0.0.1:" + DFLT_IPC_PORT);

        checkInvalid("");
        checkInvalid("127.0.0.1");
        checkInvalid("127.0.0.1:" + DFLT_IPC_PORT);
    }

    /**
     * Tests for Grid having normal name and IGFS having {@code null} name.
     *
     * @throws Exception If failed.
     */
    public void testHandshakeDefaultIgfs() throws Exception {
        startUp(false, true);

        checkInvalid(IGFS_NAME + ":" + GRID_NAME + "@");
        checkInvalid(IGFS_NAME + ":" + GRID_NAME + "@127.0.0.1");
        checkInvalid(IGFS_NAME + ":" + GRID_NAME + "@127.0.0.1:" + DFLT_IPC_PORT);

        checkInvalid(IGFS_NAME + "@");
        checkInvalid(IGFS_NAME + "@127.0.0.1");
        checkInvalid(IGFS_NAME + "@127.0.0.1:" + DFLT_IPC_PORT);

        checkValid(":" + GRID_NAME + "@");
        checkValid(":" + GRID_NAME + "@127.0.0.1");
        checkValid(":" + GRID_NAME + "@127.0.0.1:" + DFLT_IPC_PORT);

        checkInvalid("");
        checkInvalid("127.0.0.1");
        checkInvalid("127.0.0.1:" + DFLT_IPC_PORT);
    }

    /**
     * Tests for Grid having {@code null} name and IGFS having {@code null} name.
     *
     * @throws Exception If failed.
     */
    public void testHandshakeDefaultGridDefaultIgfs() throws Exception {
        startUp(true, true);

        checkInvalid(IGFS_NAME + ":" + GRID_NAME + "@");
        checkInvalid(IGFS_NAME + ":" + GRID_NAME + "@127.0.0.1");
        checkInvalid(IGFS_NAME + ":" + GRID_NAME + "@127.0.0.1:" + DFLT_IPC_PORT);

        checkInvalid(IGFS_NAME + "@");
        checkInvalid(IGFS_NAME + "@127.0.0.1");
        checkInvalid(IGFS_NAME + "@127.0.0.1:" + DFLT_IPC_PORT);

        checkInvalid(":" + GRID_NAME + "@");
        checkInvalid(":" + GRID_NAME + "@127.0.0.1");
        checkInvalid(":" + GRID_NAME + "@127.0.0.1:" + DFLT_IPC_PORT);

        checkValid("");
        checkValid("127.0.0.1");
        checkValid("127.0.0.1:" + DFLT_IPC_PORT);
    }

    /**
     * Perform startup.
     *
     * @param dfltGridName Default Grid name.
     * @param dfltIgfsName Default IGFS name.
     * @throws Exception If failed.
     */
    private void startUp(boolean dfltGridName, boolean dfltIgfsName) throws Exception {
        Ignite ignite = G.start(gridConfiguration(dfltGridName, dfltIgfsName));

        IgniteFileSystem igfs = ignite.fileSystem(dfltIgfsName ? null : IGFS_NAME);

        igfs.mkdirs(PATH);
    }

    /**
     * Create Grid configuration.
     *
     * @param dfltGridName Default Grid name.
     * @param dfltIgfsName Default IGFS name.
     * @return Grid configuration.
     * @throws Exception If failed.
     */
    private IgniteConfiguration gridConfiguration(boolean dfltGridName, boolean dfltIgfsName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(dfltGridName ? null : GRID_NAME);

        cfg.setLocalHost("127.0.0.1");
        cfg.setConnectorConfiguration(null);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        commSpi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(commSpi);

        CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setName("replicated");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        metaCacheCfg.setQueryIndexEnabled(false);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        CacheConfiguration dataCacheCfg = defaultCacheConfiguration();

        dataCacheCfg.setName("partitioned");
        dataCacheCfg.setCacheMode(PARTITIONED);
        dataCacheCfg.setDistributionMode(PARTITIONED_ONLY);
        dataCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        dataCacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(128));
        dataCacheCfg.setBackups(0);
        dataCacheCfg.setQueryIndexEnabled(false);
        dataCacheCfg.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheConfiguration(metaCacheCfg, dataCacheCfg);

        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setDataCacheName("partitioned");
        igfsCfg.setMetaCacheName("replicated");
        igfsCfg.setName(dfltIgfsName ? null : IGFS_NAME);
        igfsCfg.setPrefetchBlocks(1);
        igfsCfg.setDefaultMode(PRIMARY);

        IgfsIpcEndpointConfiguration endpointCfg = new IgfsIpcEndpointConfiguration();

        endpointCfg.setType(IgfsIpcEndpointType.TCP);
        endpointCfg.setPort(DFLT_IPC_PORT);

        igfsCfg.setIpcEndpointConfiguration(endpointCfg);

        igfsCfg.setManagementPort(-1);
        igfsCfg.setBlockSize(512 * 1024);

        cfg.setFileSystemConfiguration(igfsCfg);

        return cfg;
    }

    /**
     * Check valid file system endpoint.
     *
     * @param authority Authority.
     * @throws Exception If failed.
     */
    private void checkValid(String authority) throws Exception {
        FileSystem fs = fileSystem(authority);

        assert fs.exists(new Path(PATH.toString()));
    }

    /**
     * Check invalid file system endpoint.
     *
     * @param authority Authority.
     * @throws Exception If failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    private void checkInvalid(final String authority) throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                fileSystem(authority);

                return null;
            }
        }, IOException.class, null);
    }

    /**
     *
     *
     * @param authority Authority.
     * @return File system.
     * @throws Exception If failed.
     */
    private static FileSystem fileSystem(String authority) throws Exception {
        return FileSystem.get(new URI("igfs://" + authority + "/"), configuration(authority));
    }

    /**
     * Create configuration for test.
     *
     * @param authority Authority.
     * @return Configuration.
     */
    private static Configuration configuration(String authority) {
        Configuration cfg = new Configuration();

        cfg.set("fs.defaultFS", "igfs://" + authority + "/");
        cfg.set("fs.igfs.impl", org.apache.ignite.hadoop.fs.v1.IgniteHadoopFileSystem.class.getName());
        cfg.set("fs.AbstractFileSystem.igfs.impl",
            IgniteHadoopFileSystem.class.getName());

        cfg.setBoolean("fs.igfs.impl.disable.cache", true);

        cfg.setBoolean(String.format(PARAM_IGFS_ENDPOINT_NO_EMBED, authority), true);
        cfg.setBoolean(String.format(PARAM_IGFS_ENDPOINT_NO_LOCAL_SHMEM, authority), true);

        return cfg;
    }
}
