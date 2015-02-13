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

package org.apache.ignite.ignitefs;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.processors.fs.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.ignitefs.IgfsMode.*;
import static org.apache.ignite.ignitefs.hadoop.IgfsHadoopParameters.*;
import static org.apache.ignite.internal.processors.fs.IgfsAbstractSelfTest.*;

/**
 * Tests for GGFS working in mode when remote file system exists: DUAL_SYNC, DUAL_ASYNC.
 */
public abstract class IgfsHadoopDualAbstractSelfTest extends IgfsCommonAbstractTest {
    /** GGFS block size. */
    protected static final int GGFS_BLOCK_SIZE = 512 * 1024;

    /** Amount of blocks to prefetch. */
    protected static final int PREFETCH_BLOCKS = 1;

    /** Amount of sequential block reads before prefetch is triggered. */
    protected static final int SEQ_READS_BEFORE_PREFETCH = 2;

    /** Secondary file system URI. */
    protected static final String SECONDARY_URI = "ggfs://ggfs-secondary:grid-secondary@127.0.0.1:11500/";

    /** Secondary file system configuration path. */
    protected static final String SECONDARY_CFG = "modules/core/src/test/config/hadoop/core-site-loopback-secondary.xml";

    /** Primary file system URI. */
    protected static final String PRIMARY_URI = "ggfs://ggfs:grid@/";

    /** Primary file system configuration path. */
    protected static final String PRIMARY_CFG = "modules/core/src/test/config/hadoop/core-site-loopback.xml";

    /** Primary file system REST endpoint configuration map. */
    protected static final Map<String, String> PRIMARY_REST_CFG = new HashMap<String, String>() {{
        put("type", "tcp");
        put("port", "10500");
    }};

    /** Secondary file system REST endpoint configuration map. */
    protected static final Map<String, String> SECONDARY_REST_CFG = new HashMap<String, String>() {{
        put("type", "tcp");
        put("port", "11500");
    }};

    /** Directory. */
    protected static final IgfsPath DIR = new IgfsPath("/dir");

    /** Sub-directory. */
    protected static final IgfsPath SUBDIR = new IgfsPath(DIR, "subdir");

    /** File. */
    protected static final IgfsPath FILE = new IgfsPath(SUBDIR, "file");

    /** Default data chunk (128 bytes). */
    protected static byte[] chunk;

    /** Primary GGFS. */
    protected static IgfsImpl ggfs;

    /** Secondary GGFS. */
    protected static IgfsImpl ggfsSecondary;

    /** GGFS mode. */
    protected final IgfsMode mode;

    /**
     * Constructor.
     *
     * @param mode GGFS mode.
     */
    protected IgfsHadoopDualAbstractSelfTest(IgfsMode mode) {
        this.mode = mode;
        assert mode == DUAL_SYNC || mode == DUAL_ASYNC;
    }

    /**
     * Start grid with GGFS.
     *
     * @param gridName Grid name.
     * @param ggfsName GGFS name
     * @param mode GGFS mode.
     * @param secondaryFs Secondary file system (optional).
     * @param restCfg Rest configuration string (optional).
     * @return Started grid instance.
     * @throws Exception If failed.
     */
    protected Ignite startGridWithGgfs(String gridName, String ggfsName, IgfsMode mode,
        @Nullable Igfs secondaryFs, @Nullable Map<String, String> restCfg) throws Exception {
        IgniteFsConfiguration ggfsCfg = new IgniteFsConfiguration();

        ggfsCfg.setDataCacheName("dataCache");
        ggfsCfg.setMetaCacheName("metaCache");
        ggfsCfg.setName(ggfsName);
        ggfsCfg.setBlockSize(GGFS_BLOCK_SIZE);
        ggfsCfg.setDefaultMode(mode);
        ggfsCfg.setIpcEndpointConfiguration(restCfg);
        ggfsCfg.setSecondaryFileSystem(secondaryFs);
        ggfsCfg.setPrefetchBlocks(PREFETCH_BLOCKS);
        ggfsCfg.setSequentialReadsBeforePrefetch(SEQ_READS_BEFORE_PREFETCH);

        CacheConfiguration dataCacheCfg = defaultCacheConfiguration();

        dataCacheCfg.setName("dataCache");
        dataCacheCfg.setCacheMode(PARTITIONED);
        dataCacheCfg.setDistributionMode(CacheDistributionMode.PARTITIONED_ONLY);
        dataCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        dataCacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(2));
        dataCacheCfg.setBackups(0);
        dataCacheCfg.setQueryIndexEnabled(false);
        dataCacheCfg.setAtomicityMode(TRANSACTIONAL);
        dataCacheCfg.setOffHeapMaxMemory(0);

        CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setName("metaCache");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setQueryIndexEnabled(false);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(dataCacheCfg, metaCacheCfg);
        cfg.setGgfsConfiguration(ggfsCfg);

        cfg.setLocalHost("127.0.0.1");
        cfg.setConnectorConfiguration(null);

        return G.start(cfg);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        chunk = new byte[128];

        for (int i = 0; i < chunk.length; i++)
            chunk[i] = (byte)i;

        Ignite igniteSecondary = startGridWithGgfs("grid-secondary", "ggfs-secondary", PRIMARY, null, SECONDARY_REST_CFG);

        Igfs hadoopFs = new IgfsHadoopFileSystemWrapper(SECONDARY_URI, SECONDARY_CFG);

        Ignite ignite = startGridWithGgfs("grid", "ggfs", mode, hadoopFs, PRIMARY_REST_CFG);

        ggfsSecondary = (IgfsImpl) igniteSecondary.fileSystem("ggfs-secondary");
        ggfs = (IgfsImpl) ignite.fileSystem("ggfs");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        clear(ggfs);
        clear(ggfsSecondary);
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
        create(ggfsSecondary, paths(DIR, SUBDIR), paths(FILE));

        // Write enough data to the secondary file system.
        final int blockSize = GGFS_BLOCK_SIZE;

        IgfsOutputStream out = ggfsSecondary.append(FILE, false);

        int totalWritten = 0;

        while (totalWritten < blockSize * 2 + chunk.length) {
            out.write(chunk);

            totalWritten += chunk.length;
        }

        out.close();

        awaitFileClose(ggfsSecondary, FILE);

        // Instantiate file system with overridden "seq reads before prefetch" property.
        Configuration cfg = new Configuration();

        cfg.addResource(U.resolveIgniteUrl(PRIMARY_CFG));

        int seqReads = SEQ_READS_BEFORE_PREFETCH + 1;

        cfg.setInt(String.format(PARAM_GGFS_SEQ_READS_BEFORE_PREFETCH, "ggfs:grid@"), seqReads);

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
        IgfsMetaManager meta = ggfs.context().meta();

        IgfsFileInfo info = meta.info(meta.fileId(FILE));

        IgfsBlockKey key = new IgfsBlockKey(info.id(), info.affinityKey(), info.evictExclude(), 2);

        GridCache<IgfsBlockKey, byte[]> dataCache = ggfs.context().kernalContext().cache().cache(
            ggfs.configuration().getDataCacheName());

        for (int i = 0; i < 10; i++) {
            if (dataCache.containsKey(key))
                break;
            else
                U.sleep(100);
        }

        fsIn.close();

        // Remove the file from the secondary file system.
        ggfsSecondary.delete(FILE, false);

        // Try reading the third block. Should fail.
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgfsInputStream in0 = ggfs.open(FILE);

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
