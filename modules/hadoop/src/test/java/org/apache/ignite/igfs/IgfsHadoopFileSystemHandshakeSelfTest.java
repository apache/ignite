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
import org.apache.ignite.igfs.hadoop.v2.IgfsHadoopFileSystem;
import org.apache.ignite.internal.processors.fs.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;
import static org.apache.ignite.igfs.IgfsMode.*;
import static org.apache.ignite.internal.fs.hadoop.IgfsHadoopUtils.*;
import static org.apache.ignite.internal.util.ipc.shmem.IpcSharedMemoryServerEndpoint.*;

/**
 * Tests for GGFS file system handshake.
 */
public class IgfsHadoopFileSystemHandshakeSelfTest extends IgfsCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Grid name. */
    private static final String GRID_NAME = "grid";

    /** GGFS name. */
    private static final String GGFS_NAME = "ggfs";

    /** GGFS path. */
    private static final IgfsPath PATH = new IgfsPath("/path");

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);
    }

    /**
     * Tests for Grid and GGFS having normal names.
     *
     * @throws Exception If failed.
     */
    public void testHandshake() throws Exception {
        startUp(false, false);

        checkValid(GGFS_NAME + ":" + GRID_NAME + "@");
        checkValid(GGFS_NAME + ":" + GRID_NAME + "@127.0.0.1");
        checkValid(GGFS_NAME + ":" + GRID_NAME + "@127.0.0.1:" + DFLT_IPC_PORT);

        checkInvalid(GGFS_NAME + "@");
        checkInvalid(GGFS_NAME + "@127.0.0.1");
        checkInvalid(GGFS_NAME + "@127.0.0.1:" + DFLT_IPC_PORT);

        checkInvalid(":" + GRID_NAME + "@");
        checkInvalid(":" + GRID_NAME + "@127.0.0.1");
        checkInvalid(":" + GRID_NAME + "@127.0.0.1:" + DFLT_IPC_PORT);

        checkInvalid("");
        checkInvalid("127.0.0.1");
        checkInvalid("127.0.0.1:" + DFLT_IPC_PORT);
    }

    /**
     * Tests for Grid having {@code null} name and GGFS having normal name.
     *
     * @throws Exception If failed.
     */
    public void testHandshakeDefaultGrid() throws Exception {
        startUp(true, false);

        checkInvalid(GGFS_NAME + ":" + GRID_NAME + "@");
        checkInvalid(GGFS_NAME + ":" + GRID_NAME + "@127.0.0.1");
        checkInvalid(GGFS_NAME + ":" + GRID_NAME + "@127.0.0.1:" + DFLT_IPC_PORT);

        checkValid(GGFS_NAME + "@");
        checkValid(GGFS_NAME + "@127.0.0.1");
        checkValid(GGFS_NAME + "@127.0.0.1:" + DFLT_IPC_PORT);

        checkInvalid(":" + GRID_NAME + "@");
        checkInvalid(":" + GRID_NAME + "@127.0.0.1");
        checkInvalid(":" + GRID_NAME + "@127.0.0.1:" + DFLT_IPC_PORT);

        checkInvalid("");
        checkInvalid("127.0.0.1");
        checkInvalid("127.0.0.1:" + DFLT_IPC_PORT);
    }

    /**
     * Tests for Grid having normal name and GGFS having {@code null} name.
     *
     * @throws Exception If failed.
     */
    public void testHandshakeDefaultGgfs() throws Exception {
        startUp(false, true);

        checkInvalid(GGFS_NAME + ":" + GRID_NAME + "@");
        checkInvalid(GGFS_NAME + ":" + GRID_NAME + "@127.0.0.1");
        checkInvalid(GGFS_NAME + ":" + GRID_NAME + "@127.0.0.1:" + DFLT_IPC_PORT);

        checkInvalid(GGFS_NAME + "@");
        checkInvalid(GGFS_NAME + "@127.0.0.1");
        checkInvalid(GGFS_NAME + "@127.0.0.1:" + DFLT_IPC_PORT);

        checkValid(":" + GRID_NAME + "@");
        checkValid(":" + GRID_NAME + "@127.0.0.1");
        checkValid(":" + GRID_NAME + "@127.0.0.1:" + DFLT_IPC_PORT);

        checkInvalid("");
        checkInvalid("127.0.0.1");
        checkInvalid("127.0.0.1:" + DFLT_IPC_PORT);
    }

    /**
     * Tests for Grid having {@code null} name and GGFS having {@code null} name.
     *
     * @throws Exception If failed.
     */
    public void testHandshakeDefaultGridDefaultGgfs() throws Exception {
        startUp(true, true);

        checkInvalid(GGFS_NAME + ":" + GRID_NAME + "@");
        checkInvalid(GGFS_NAME + ":" + GRID_NAME + "@127.0.0.1");
        checkInvalid(GGFS_NAME + ":" + GRID_NAME + "@127.0.0.1:" + DFLT_IPC_PORT);

        checkInvalid(GGFS_NAME + "@");
        checkInvalid(GGFS_NAME + "@127.0.0.1");
        checkInvalid(GGFS_NAME + "@127.0.0.1:" + DFLT_IPC_PORT);

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
     * @param dfltGgfsName Default GGFS name.
     * @throws Exception If failed.
     */
    private void startUp(boolean dfltGridName, boolean dfltGgfsName) throws Exception {
        Ignite ignite = G.start(gridConfiguration(dfltGridName, dfltGgfsName));

        IgniteFs ggfs = ignite.fileSystem(dfltGgfsName ? null : GGFS_NAME);

        ggfs.mkdirs(PATH);
    }

    /**
     * Create Grid configuration.
     *
     * @param dfltGridName Default Grid name.
     * @param dfltGgfsName Default GGFS name.
     * @return Grid configuration.
     * @throws Exception If failed.
     */
    private IgniteConfiguration gridConfiguration(boolean dfltGridName, boolean dfltGgfsName) throws Exception {
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

        IgfsConfiguration ggfsCfg = new IgfsConfiguration();

        ggfsCfg.setDataCacheName("partitioned");
        ggfsCfg.setMetaCacheName("replicated");
        ggfsCfg.setName(dfltGgfsName ? null : GGFS_NAME);
        ggfsCfg.setPrefetchBlocks(1);
        ggfsCfg.setDefaultMode(PRIMARY);
        ggfsCfg.setIpcEndpointConfiguration(new HashMap<String, String>() {{
            put("type", "tcp");
            put("port", String.valueOf(DFLT_IPC_PORT));
        }});

        ggfsCfg.setManagementPort(-1);
        ggfsCfg.setBlockSize(512 * 1024);

        cfg.setGgfsConfiguration(ggfsCfg);

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
        return FileSystem.get(new URI("ggfs://" + authority + "/"), configuration(authority));
    }

    /**
     * Create configuration for test.
     *
     * @param authority Authority.
     * @return Configuration.
     */
    private static Configuration configuration(String authority) {
        Configuration cfg = new Configuration();

        cfg.set("fs.defaultFS", "ggfs://" + authority + "/");
        cfg.set("fs.ggfs.impl", org.apache.ignite.igfs.hadoop.v1.IgfsHadoopFileSystem.class.getName());
        cfg.set("fs.AbstractFileSystem.ggfs.impl",
            IgfsHadoopFileSystem.class.getName());

        cfg.setBoolean("fs.ggfs.impl.disable.cache", true);

        cfg.setBoolean(String.format(PARAM_GGFS_ENDPOINT_NO_EMBED, authority), true);
        cfg.setBoolean(String.format(PARAM_GGFS_ENDPOINT_NO_LOCAL_SHMEM, authority), true);

        return cfg;
    }
}
