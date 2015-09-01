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

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.Callable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.hadoop.fs.IgniteHadoopIgfsSecondaryFileSystem;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.internal.processors.igfs.IgfsBlockKey;
import org.apache.ignite.internal.processors.igfs.IgfsCommonAbstractTest;
import org.apache.ignite.internal.processors.igfs.IgfsFileInfo;
import org.apache.ignite.internal.processors.igfs.IgfsImpl;
import org.apache.ignite.internal.processors.igfs.IgfsMetaManager;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.igfs.IgfsMode.DUAL_ASYNC;
import static org.apache.ignite.igfs.IgfsMode.DUAL_SYNC;
import static org.apache.ignite.igfs.IgfsMode.PRIMARY;
import static org.apache.ignite.internal.processors.hadoop.fs.HadoopParameters.PARAM_IGFS_SEQ_READS_BEFORE_PREFETCH;
import static org.apache.ignite.internal.processors.igfs.IgfsAbstractSelfTest.awaitFileClose;
import static org.apache.ignite.internal.processors.igfs.IgfsAbstractSelfTest.clear;
import static org.apache.ignite.internal.processors.igfs.IgfsAbstractSelfTest.create;

/**
 * Tests for IGFS working in mode when remote file system exists: DUAL_SYNC, DUAL_ASYNC.
 */
public abstract class HadoopIgfsDualAbstractSelfTest extends IgfsCommonAbstractTest {
    /** IGFS block size. */
    protected static final int IGFS_BLOCK_SIZE = 512 * 1024;

    /** Amount of blocks to prefetch. */
    protected static final int PREFETCH_BLOCKS = 1;

    /** Amount of sequential block reads before prefetch is triggered. */
    protected static final int SEQ_READS_BEFORE_PREFETCH = 2;

    /** Secondary file system URI. */
    protected static final String SECONDARY_URI = "igfs://igfs-secondary:grid-secondary@127.0.0.1:11500/";

    /** Secondary file system configuration path. */
    protected static final String SECONDARY_CFG = "modules/core/src/test/config/hadoop/core-site-loopback-secondary.xml";

    /** Primary file system URI. */
    protected static final String PRIMARY_URI = "igfs://igfs:grid@/";

    /** Primary file system configuration path. */
    protected static final String PRIMARY_CFG = "modules/core/src/test/config/hadoop/core-site-loopback.xml";

    /** Primary file system REST endpoint configuration map. */
    protected static final IgfsIpcEndpointConfiguration PRIMARY_REST_CFG;

    /** Secondary file system REST endpoint configuration map. */
    protected static final IgfsIpcEndpointConfiguration SECONDARY_REST_CFG;

    /** Directory. */
    protected static final IgfsPath DIR = new IgfsPath("/dir");

    /** Sub-directory. */
    protected static final IgfsPath SUBDIR = new IgfsPath(DIR, "subdir");

    /** File. */
    protected static final IgfsPath FILE = new IgfsPath(SUBDIR, "file");

    /** Default data chunk (128 bytes). */
    protected static byte[] chunk;

    /** Primary IGFS. */
    protected static IgfsImpl igfs;

    /** Secondary IGFS. */
    protected static IgfsImpl igfsSecondary;

    /** IGFS mode. */
    protected final IgfsMode mode;

    static {
        PRIMARY_REST_CFG = new IgfsIpcEndpointConfiguration();

        PRIMARY_REST_CFG.setType(IgfsIpcEndpointType.TCP);
        PRIMARY_REST_CFG.setPort(10500);

        SECONDARY_REST_CFG = new IgfsIpcEndpointConfiguration();

        SECONDARY_REST_CFG.setType(IgfsIpcEndpointType.TCP);
        SECONDARY_REST_CFG.setPort(11500);
    }

    /**
     * Constructor.
     *
     * @param mode IGFS mode.
     */
    protected HadoopIgfsDualAbstractSelfTest(IgfsMode mode) {
        this.mode = mode;
        assert mode == DUAL_SYNC || mode == DUAL_ASYNC;
    }

    /**
     * Start grid with IGFS.
     *
     * @param gridName Grid name.
     * @param igfsName IGFS name
     * @param mode IGFS mode.
     * @param secondaryFs Secondary file system (optional).
     * @param restCfg Rest configuration string (optional).
     * @return Started grid instance.
     * @throws Exception If failed.
     */
    protected Ignite startGridWithIgfs(String gridName, String igfsName, IgfsMode mode,
        @Nullable IgfsSecondaryFileSystem secondaryFs, @Nullable IgfsIpcEndpointConfiguration restCfg) throws Exception {
        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setDataCacheName("dataCache");
        igfsCfg.setMetaCacheName("metaCache");
        igfsCfg.setName(igfsName);
        igfsCfg.setBlockSize(IGFS_BLOCK_SIZE);
        igfsCfg.setDefaultMode(mode);
        igfsCfg.setIpcEndpointConfiguration(restCfg);
        igfsCfg.setSecondaryFileSystem(secondaryFs);
        igfsCfg.setPrefetchBlocks(PREFETCH_BLOCKS);
        igfsCfg.setSequentialReadsBeforePrefetch(SEQ_READS_BEFORE_PREFETCH);

        CacheConfiguration dataCacheCfg = defaultCacheConfiguration();

        dataCacheCfg.setName("dataCache");
        dataCacheCfg.setCacheMode(PARTITIONED);
        dataCacheCfg.setNearConfiguration(null);
        dataCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        dataCacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(2));
        dataCacheCfg.setBackups(0);
        dataCacheCfg.setAtomicityMode(TRANSACTIONAL);
        dataCacheCfg.setOffHeapMaxMemory(0);

        CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setName("metaCache");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(dataCacheCfg, metaCacheCfg);
        cfg.setFileSystemConfiguration(igfsCfg);

        cfg.setLocalHost("127.0.0.1");
        cfg.setConnectorConfiguration(null);

        return G.start(cfg);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        chunk = new byte[128];

        for (int i = 0; i < chunk.length; i++)
            chunk[i] = (byte)i;

        Ignite igniteSecondary = startGridWithIgfs("grid-secondary", "igfs-secondary", PRIMARY, null, SECONDARY_REST_CFG);

        IgfsSecondaryFileSystem hadoopFs = new IgniteHadoopIgfsSecondaryFileSystem(SECONDARY_URI, SECONDARY_CFG);

        Ignite ignite = startGridWithIgfs("grid", "igfs", mode, hadoopFs, PRIMARY_REST_CFG);

        igfsSecondary = (IgfsImpl) igniteSecondary.fileSystem("igfs-secondary");
        igfs = (IgfsImpl) ignite.fileSystem("igfs");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        clear(igfs);
        clear(igfsSecondary);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        G.stopAll(true);
    }

    /**
     * Convenient method to group paths.
     *
     * @param paths Paths to group.
     * @return Paths as array.
     */
    protected IgfsPath[] paths(IgfsPath... paths) {
        return paths;
    }

    /**
     * Check how prefetch override works.
     *
     * @throws Exception IF failed.
     */
    public void testOpenPrefetchOverride() throws Exception {
        create(igfsSecondary, paths(DIR, SUBDIR), paths(FILE));

        // Write enough data to the secondary file system.
        final int blockSize = IGFS_BLOCK_SIZE;

        IgfsOutputStream out = igfsSecondary.append(FILE, false);

        int totalWritten = 0;

        while (totalWritten < blockSize * 2 + chunk.length) {
            out.write(chunk);

            totalWritten += chunk.length;
        }

        out.close();

        awaitFileClose(igfsSecondary.asSecondary(), FILE);

        // Instantiate file system with overridden "seq reads before prefetch" property.
        Configuration cfg = new Configuration();

        cfg.addResource(U.resolveIgniteUrl(PRIMARY_CFG));

        int seqReads = SEQ_READS_BEFORE_PREFETCH + 1;

        cfg.setInt(String.format(PARAM_IGFS_SEQ_READS_BEFORE_PREFETCH, "igfs:grid@"), seqReads);

        FileSystem fs = FileSystem.get(new URI(PRIMARY_URI), cfg);

        // Read the first two blocks.
        Path fsHome = new Path(PRIMARY_URI);
        Path dir = new Path(fsHome, DIR.name());
        Path subdir = new Path(dir, SUBDIR.name());
        Path file = new Path(subdir, FILE.name());

        FSDataInputStream fsIn = fs.open(file);

        final byte[] readBuf = new byte[blockSize * 2];

        fsIn.readFully(0, readBuf, 0, readBuf.length);

        // Wait for a while for prefetch to finish (if any).
        IgfsMetaManager meta = igfs.context().meta();

        IgfsFileInfo info = meta.info(meta.fileId(FILE));

        IgfsBlockKey key = new IgfsBlockKey(info.id(), info.affinityKey(), info.evictExclude(), 2);

        IgniteCache<IgfsBlockKey, byte[]> dataCache = igfs.context().kernalContext().cache().jcache(
            igfs.configuration().getDataCacheName());

        for (int i = 0; i < 10; i++) {
            if (dataCache.containsKey(key))
                break;
            else
                U.sleep(100);
        }

        fsIn.close();

        // Remove the file from the secondary file system.
        igfsSecondary.delete(FILE, false);

        // Try reading the third block. Should fail.
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgfsInputStream in0 = igfs.open(FILE);

                in0.seek(blockSize * 2);

                try {
                    in0.read(readBuf);
                }
                finally {
                    U.closeQuiet(in0);
                }

                return null;
            }
        }, IOException.class,
            "Failed to read data due to secondary file system exception: /dir/subdir/file");
    }
}