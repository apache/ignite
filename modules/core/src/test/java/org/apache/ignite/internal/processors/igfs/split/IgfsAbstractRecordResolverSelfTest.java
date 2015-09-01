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

package org.apache.ignite.internal.processors.igfs.split;

import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.igfs.IgfsInputStream;
import org.apache.ignite.igfs.IgfsOutputStream;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.mapreduce.IgfsFileRange;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.igfs.IgfsMode.PRIMARY;

/**
 * Base class for all split resolvers
 */
public class IgfsAbstractRecordResolverSelfTest extends GridCommonAbstractTest {
    /** File path. */
    protected static final IgfsPath FILE = new IgfsPath("/file");

    /** Shared IP finder. */
    private final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** IGFS. */
    protected static IgniteFileSystem igfs;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setDataCacheName("dataCache");
        igfsCfg.setMetaCacheName("metaCache");
        igfsCfg.setName("igfs");
        igfsCfg.setBlockSize(512);
        igfsCfg.setDefaultMode(PRIMARY);

        CacheConfiguration dataCacheCfg = new CacheConfiguration();

        dataCacheCfg.setName("dataCache");
        dataCacheCfg.setCacheMode(PARTITIONED);
        dataCacheCfg.setAtomicityMode(TRANSACTIONAL);
        dataCacheCfg.setNearConfiguration(new NearCacheConfiguration());
        dataCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        dataCacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(128));
        dataCacheCfg.setBackups(0);

        CacheConfiguration metaCacheCfg = new CacheConfiguration();

        metaCacheCfg.setName("metaCache");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);
        metaCacheCfg.setWriteSynchronizationMode(FULL_SYNC);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName("grid");

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(dataCacheCfg, metaCacheCfg);
        cfg.setFileSystemConfiguration(igfsCfg);

        Ignite g = G.start(cfg);

        igfs = g.fileSystem("igfs");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids(false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        igfs.format();
    }

    /**
     * Convenient method for wrapping some bytes into byte array.
     *
     * @param data Data bytes.
     * @return Byte array.
     */
    protected static byte[] wrap(int... data) {
        byte[] res = new byte[data.length];

        for (int i = 0; i < data.length; i++)
            res[i] = (byte)data[i];

        return res;
    }

    /**
     * Create byte array consisting of the given chunks.
     *
     * @param chunks Array of chunks where the first value is the byte array and the second value is amount of repeats.
     * @return Byte array.
     */
    protected static byte[] array(Map.Entry<byte[], Integer>... chunks) {
        int totalSize = 0;

        for (Map.Entry<byte[], Integer> chunk : chunks)
            totalSize += chunk.getKey().length * chunk.getValue();

        byte[] res = new byte[totalSize];

        int pos = 0;

        for (Map.Entry<byte[], Integer> chunk : chunks) {
            for (int i = 0; i < chunk.getValue(); i++) {
                System.arraycopy(chunk.getKey(), 0, res, pos, chunk.getKey().length);

                pos += chunk.getKey().length;
            }
        }

        return res;
    }

    /**
     * Open file for read and return input stream.
     *
     * @return Input stream.
     * @throws Exception In case of exception.
     */
    protected IgfsInputStream read() throws Exception {
        return igfs.open(FILE);
    }

    /**
     * Write data to the file.
     *
     * @param chunks Data chunks.
     * @throws Exception In case of exception.
     */
    protected void write(byte[]... chunks) throws Exception {
        IgfsOutputStream os =  igfs.create(FILE, true);

        if (chunks != null) {
            for (byte[] chunk : chunks)
                os.write(chunk);
        }

        os.close();
    }

    /**
     * Create split.
     *
     * @param start Start position.
     * @param len Length.
     * @return Split.
     */
    protected IgfsFileRange split(long start, long len) {
        return new IgfsFileRange(FILE, start, len);
    }
}