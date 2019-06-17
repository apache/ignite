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

package org.apache.ignite.internal.processors.hadoop.impl.igfs;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.Callable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.hadoop.fs.v2.IgniteHadoopFileSystem;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.igfs.IgfsIpcEndpointConfiguration;
import org.apache.ignite.igfs.IgfsIpcEndpointType;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.processors.igfs.IgfsCommonAbstractTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.igfs.IgfsMode.PRIMARY;
import static org.apache.ignite.internal.processors.hadoop.impl.igfs.HadoopIgfsUtils.PARAM_IGFS_ENDPOINT_NO_EMBED;
import static org.apache.ignite.internal.processors.hadoop.impl.igfs.HadoopIgfsUtils.PARAM_IGFS_ENDPOINT_NO_LOCAL_SHMEM;
import static org.apache.ignite.internal.processors.hadoop.impl.igfs.HadoopIgfsUtils.PARAM_IGFS_ENDPOINT_NO_LOCAL_TCP;
import static org.apache.ignite.internal.util.ipc.shmem.IpcSharedMemoryServerEndpoint.DFLT_IPC_PORT;

/**
 * Tests for IGFS file system handshake.
 */
public class IgniteHadoopFileSystemHandshakeSelfTest extends IgfsCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Ignite instance name. */
    private static final String IGNITE_INSTANCE_NAME = "grid";

    /** IGFS name. */
    private static final String IGFS_NAME = "igfs";

    /** IGFS path. */
    private static final IgfsPath PATH = new IgfsPath("/path");

    /** A host-port pair used for URI in embedded mode. */
    private static final String HOST_PORT_UNUSED = "somehost:65333";

    /** Flag defines if to use TCP or embedded connection mode: */
    private boolean tcp = false;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);
    }

    /**
     * Tests for Grid and IGFS having normal names.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testHandshake() throws Exception {
        startUp(false, false);

        tcp = true;

        checkValid(IGFS_NAME + ":" + IGNITE_INSTANCE_NAME + "@");
        checkValid(IGFS_NAME + ":" + IGNITE_INSTANCE_NAME + "@127.0.0.1");
        checkValid(IGFS_NAME + ":" + IGNITE_INSTANCE_NAME + "@127.0.0.1:" + DFLT_IPC_PORT);

        checkValid(IGFS_NAME + "@");
        checkValid(IGFS_NAME + "@127.0.0.1");
        checkValid(IGFS_NAME + "@127.0.0.1:" + DFLT_IPC_PORT);

        tcp = false; // Embedded mode:

        checkValid(IGFS_NAME + ":" + IGNITE_INSTANCE_NAME + "@");
        checkValid(IGFS_NAME + ":" + IGNITE_INSTANCE_NAME + "@" + HOST_PORT_UNUSED);

        checkValid(IGFS_NAME + "@"); // Embedded mode fails, but remote tcp succeeds.
        checkValid(IGFS_NAME + "@" + HOST_PORT_UNUSED);
    }

    /**
     * Tests for Grid having {@code null} name and IGFS having normal name.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testHandshakeDefaultGrid() throws Exception {
        startUp(true, false);

        tcp = true;

        checkValid(IGFS_NAME + ":" + IGNITE_INSTANCE_NAME + "@");
        checkValid(IGFS_NAME + ":" + IGNITE_INSTANCE_NAME + "@127.0.0.1");
        checkValid(IGFS_NAME + ":" + IGNITE_INSTANCE_NAME + "@127.0.0.1:" + DFLT_IPC_PORT);

        checkValid(IGFS_NAME + "@");
        checkValid(IGFS_NAME + "@127.0.0.1");
        checkValid(IGFS_NAME + "@127.0.0.1:" + DFLT_IPC_PORT);

        tcp = false; // Embedded mode:

        checkValid(IGFS_NAME + ":" + IGNITE_INSTANCE_NAME + "@");
        checkValid(IGFS_NAME + ":" + IGNITE_INSTANCE_NAME + "@" + HOST_PORT_UNUSED);

        checkValid(IGFS_NAME + "@");
        checkValid(IGFS_NAME + "@" + HOST_PORT_UNUSED);
    }

    /**
     * Perform startup.
     *
     * @param dfltIgniteInstanceName Default Ignite instance name.
     * @param dfltIgfsName Default IGFS name.
     * @throws Exception If failed.
     */
    private void startUp(boolean dfltIgniteInstanceName, boolean dfltIgfsName) throws Exception {
        Ignite ignite = G.start(gridConfiguration(dfltIgniteInstanceName));

        IgniteFileSystem igfs = ignite.fileSystem(dfltIgfsName ? "" : IGFS_NAME);

        igfs.mkdirs(PATH);
    }

    /**
     * Create Grid configuration.
     *
     * @param dfltIgniteInstanceName Default Ignite instance name.
     * @return Grid configuration.
     * @throws Exception If failed.
     */
    private IgniteConfiguration gridConfiguration(boolean dfltIgniteInstanceName)
        throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(dfltIgniteInstanceName ? null : IGNITE_INSTANCE_NAME);

        cfg.setLocalHost("127.0.0.1");
        cfg.setConnectorConfiguration(null);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        commSpi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(commSpi);

        CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        CacheConfiguration dataCacheCfg = defaultCacheConfiguration();

        dataCacheCfg.setCacheMode(PARTITIONED);
        dataCacheCfg.setNearConfiguration(null);
        dataCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        dataCacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(128));
        dataCacheCfg.setBackups(0);
        dataCacheCfg.setAtomicityMode(TRANSACTIONAL);

        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setName(IGFS_NAME);
        igfsCfg.setPrefetchBlocks(1);
        igfsCfg.setDefaultMode(PRIMARY);
        igfsCfg.setDataCacheConfiguration(dataCacheCfg);
        igfsCfg.setMetaCacheConfiguration(metaCacheCfg);

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
        FileSystem fs = fileSystem(authority, tcp);

        assert fs.exists(new Path(PATH.toString()));
    }

    /**
     * Check invalid file system endpoint.
     *
     * @param authority Authority.
     * @throws Exception If failed.
     */
    private void checkInvalid(final String authority) throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                fileSystem(authority, tcp);

                return null;
            }
        }, IOException.class, null);
    }

    /**
     * Gets the file system using authority and tcp flag.
     *
     * @param authority Authority.
     * @param tcp Use TCP endpoint.
     * @return File system.
     * @throws Exception If failed.
     */
    private static FileSystem fileSystem(String authority, boolean tcp) throws Exception {
        return FileSystem.get(new URI("igfs://" + authority + "/"), configuration(authority, tcp));
    }

    /**
     * Create configuration for test.
     *
     * @param authority Authority.
     * @param tcp Use TCP endpoint.
     * @return Configuration.
     */
    private static Configuration configuration(String authority, boolean tcp) {
        Configuration cfg = new Configuration();

        cfg.set("fs.defaultFS", "igfs://" + authority + "/");
        cfg.set("fs.igfs.impl", org.apache.ignite.hadoop.fs.v1.IgniteHadoopFileSystem.class.getName());
        cfg.set("fs.AbstractFileSystem.igfs.impl",
            IgniteHadoopFileSystem.class.getName());

        cfg.setBoolean("fs.igfs.impl.disable.cache", true);

        if (tcp)
            cfg.setBoolean(String.format(PARAM_IGFS_ENDPOINT_NO_EMBED, authority), true);
        else
            cfg.setBoolean(String.format(PARAM_IGFS_ENDPOINT_NO_LOCAL_TCP, authority), true);

        cfg.setBoolean(String.format(PARAM_IGFS_ENDPOINT_NO_LOCAL_SHMEM, authority), true);

        return cfg;
    }
}
