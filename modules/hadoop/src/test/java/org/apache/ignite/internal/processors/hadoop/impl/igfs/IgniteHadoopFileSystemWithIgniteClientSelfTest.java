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

import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Test hadoop file system implementation.
 */
public class IgniteHadoopFileSystemWithIgniteClientSelfTest extends GridCommonAbstractTest {
    /** IGFS name */
    public static final String IGFS_NAME = "test";

    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(nodeCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        G.stopAll(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));
        cfg.setPeerClassLoadingEnabled(false);

        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setDataCacheName("data");
        igfsCfg.setMetaCacheName("meta");
        igfsCfg.setName(IGFS_NAME);

        cfg.setFileSystemConfiguration(igfsCfg);

        cfg.setCacheConfiguration(cacheConfiguration(gridName, "data"), cacheConfiguration(gridName, "meta"));

        return cfg;
    }

    /** @return Node count for test. */
    protected int nodeCount() {
        return 2;
    }

    /**
     * Gets cache configuration.
     *
     * @param gridName Grid name.
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfiguration(String gridName, String cacheName) {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName(cacheName);
        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setBackups(1);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        return cacheCfg;
    }

    /**
     * Gets config of concrete File System.
     *
     * @return Config of concrete File System.
     */
    protected Configuration getFileSystemConfig() {
        Configuration cfg = new Configuration();

        cfg.setStrings("fs.default.name", "igfs://" + IGFS_NAME + '/');

        cfg.setClass("fs.igfs.impl",
            org.apache.ignite.hadoop.fs.v1.IgniteHadoopFileSystem.class,
            FileSystem.class);
        cfg.setClass("fs.AbstractFileSystem.igfs.impl",
            org.apache.ignite.hadoop.fs.v2.IgniteHadoopFileSystem.class,
            AbstractFileSystem.class);

        String igfsAuthority = IGFS_NAME + '@';
        cfg.setBoolean(String.format(HadoopIgfsUtils.PARAM_IGFS_ENDPOINT_NO_EMBED, igfsAuthority), true);
        cfg.setBoolean(String.format(HadoopIgfsUtils.PARAM_IGFS_ENDPOINT_NO_LOCAL_SHMEM, igfsAuthority), true);
        cfg.setBoolean(String.format(HadoopIgfsUtils.PARAM_IGFS_ENDPOINT_NO_LOCAL_TCP, igfsAuthority), true);
        cfg.setBoolean(String.format(HadoopIgfsUtils.PARAM_IGFS_ENDPOINT_NO_REMOTE_TCP, igfsAuthority), true);

        cfg.setStrings(String.format(HadoopIgfsUtils.PARAM_IGFS_ENDPOINT_IGNITE_CFG_PATH, igfsAuthority),
            "modules/hadoop/src/test/config/igfs-cli-config.xml");

        return cfg;
    }

    /**
     * Gets File System name.
     *
     * @return File System name.
     */
    protected URI getFileSystemURI() {
        try {
            return new URI("igfs://" + IGFS_NAME + "@/");
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    /** @throws Exception If failed. */
    public void testContentsConsistency() throws Exception {
        try (FileSystem fs = FileSystem.get(getFileSystemURI(), getFileSystemConfig())) {
            Collection<IgniteBiTuple<String, Long>> files = F.asList(
                F.t("/dir1/dir2/file1", 1024L),
                F.t("/dir1/dir2/file2", 8 * 1024L),
                F.t("/dir1/file1", 1024 * 1024L),
                F.t("/dir1/file2", 5 * 1024 * 1024L),
                F.t("/file1", 64 * 1024L + 13),
                F.t("/file2", 13L),
                F.t("/file3", 123764L)
            );

            for (IgniteBiTuple<String, Long> file : files) {

                info("Writing file: " + file.get1());

                try (OutputStream os = fs.create(new Path(file.get1()), (short)3)) {
                    byte[] data = new byte[file.get2().intValue()];

                    data[0] = 25;
                    data[data.length - 1] = 26;

                    os.write(data);
                }

                info("Finished writing file: " + file.get1());
            }

            for (int i = 1; i < nodeCount(); i++) {

                try (FileSystem ignored = FileSystem.get(getFileSystemURI(), getFileSystemConfig())) {
                    for (IgniteBiTuple<String, Long> file : files) {
                        Path path = new Path(file.get1());

                        FileStatus fileStatus = fs.getFileStatus(path);

                        assertEquals(file.get2(), (Long)fileStatus.getLen());

                        byte[] read = new byte[file.get2().intValue()];

                        info("Reading file: " + path);

                        try (FSDataInputStream in = fs.open(path)) {
                            in.readFully(read);

                            assert read[0] == 25;
                            assert read[read.length - 1] == 26;
                        }

                        info("Finished reading file: " + path);
                    }
                }
            }
        }
    }
}