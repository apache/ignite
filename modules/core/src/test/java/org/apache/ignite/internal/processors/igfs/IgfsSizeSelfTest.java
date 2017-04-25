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

import java.io.IOException;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.igfs.IgfsInputStream;
import org.apache.ignite.igfs.IgfsOutputStream;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.jsr166.ThreadLocalRandom8;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;

/**
 * {@link IgfsAttributes} test case.
 */
public class IgfsSizeSelfTest extends IgfsCommonAbstractTest {
    /** How many grids to start. */
    private static final int GRID_CNT = 3;

    /** How many files to save. */
    private static final int FILES_CNT = 10;

    /** Maximum amount of bytes that could be written to particular file. */
    private static final int MAX_FILE_SIZE = 1024 * 10;

    /** Block size. */
    private static final int BLOCK_SIZE = 384;

    /** IGFS name. */
    private static final String IGFS_NAME = "test";

    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** IGFS management port */
    private static int mgmtPort;

    /** Data cache mode. */
    private CacheMode cacheMode;

    /** Whether near cache is enabled (applicable for PARTITIONED cache only). */
    private boolean nearEnabled;

    /** Mem policy setter. */
    private IgniteInClosure<IgniteConfiguration> memIgfsdDataPlcSetter;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cacheMode = null;
        nearEnabled = false;

        mgmtPort = 11400;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        G.stopAll(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setName(IGFS_NAME);
        igfsCfg.setBlockSize(BLOCK_SIZE);
        igfsCfg.setFragmentizerEnabled(false);
        igfsCfg.setManagementPort(++mgmtPort);

        CacheConfiguration dataCfg = defaultCacheConfiguration();

        dataCfg.setCacheMode(cacheMode);

        if (cacheMode == PARTITIONED) {
            if (nearEnabled)
                dataCfg.setNearConfiguration(new NearCacheConfiguration());

            dataCfg.setBackups(0);
        }

        dataCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        dataCfg.setRebalanceMode(SYNC);
        dataCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(128));
        dataCfg.setAtomicityMode(TRANSACTIONAL);

        CacheConfiguration metaCfg = defaultCacheConfiguration();

        metaCfg.setCacheMode(REPLICATED);

        metaCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        metaCfg.setRebalanceMode(SYNC);
        metaCfg.setAtomicityMode(TRANSACTIONAL);

        igfsCfg.setMetaCacheConfiguration(metaCfg);
        igfsCfg.setDataCacheConfiguration(dataCfg);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);
        cfg.setFileSystemConfiguration(igfsCfg);

        if (memIgfsdDataPlcSetter != null)
            memIgfsdDataPlcSetter.apply(cfg);

        return cfg;
    }

    /**
     * Perform initial startup.
     *
     * @throws Exception If failed.
     */
    private void startUp() throws Exception {
        startGrids(GRID_CNT);

        // Await for stable topology.
        awaitPartitionMapExchange();
    }

    /**
     * Ensure that PARTITIONED cache is correctly initialized.
     *
     * @throws Exception If failed.
     */
    public void testPartitioned() throws Exception {
        cacheMode = PARTITIONED;
        nearEnabled = true;

        check();
    }

    /**
     * Ensure that co-located cache is correctly initialized.
     *
     * @throws Exception If failed.
     */
    public void testColocated() throws Exception {
        cacheMode = PARTITIONED;
        nearEnabled = false;

        check();
    }

    /**
     * Ensure that REPLICATED cache is correctly initialized.
     *
     * @throws Exception If failed.
     */
    public void testReplicated() throws Exception {
        cacheMode = REPLICATED;

        check();
    }

    /**
     * Ensure that exception is thrown in case PARTITIONED cache is oversized.
     *
     * @throws Exception If failed.
     */
    public void testPartitionedOversize() throws Exception {
        cacheMode = PARTITIONED;
        nearEnabled = true;

        checkOversize();
    }

    /**
     * Ensure that exception is thrown in case co-located cache is oversized.
     *
     * @throws Exception If failed.
     */
    public void testColocatedOversize() throws Exception {
        cacheMode = PARTITIONED;
        nearEnabled = false;

        checkOversize();
    }

    /**
     * Ensure that exception is thrown in case REPLICATED cache is oversized.
     *
     * @throws Exception If failed.
     */
    public void testReplicatedOversize() throws Exception {
        cacheMode = REPLICATED;

        checkOversize();
    }

    /**
     * Ensure that IGFS size is correctly updated in case of preloading for PARTITIONED cache.
     *
     * @throws Exception If failed.
     */
    public void testPartitionedPreload() throws Exception {
        cacheMode = PARTITIONED;
        nearEnabled = true;

        checkPreload();
    }

    /**
     * Ensure that IGFS size is correctly updated in case of preloading for co-located cache.
     *
     * @throws Exception If failed.
     */
    public void testColocatedPreload() throws Exception {
        cacheMode = PARTITIONED;
        nearEnabled = false;

        checkPreload();
    }

    /**
     * Ensure that IGFS cache size is calculated correctly.
     *
     * @throws Exception If failed.
     */
    private void check() throws Exception {
        startUp();

        // Ensure that cache was marked as IGFS data cache.
        for (int i = 0; i < GRID_CNT; i++) {
            IgniteEx g = grid(i);

            IgniteInternalCache cache = g.cachex(g.igfsx(IGFS_NAME).configuration().getDataCacheConfiguration()
                .getName()).cache();

            assert cache.isIgfsDataCache();
        }

        // Perform writes.
        Collection<IgfsFile> files = write();

        // Check sizes.
        Map<UUID, Integer> expSizes = new HashMap<>(GRID_CNT, 1.0f);

        for (IgfsFile file : files) {
            for (IgfsBlock block : file.blocks()) {
                Collection<UUID> ids = primaryOrBackups(block.key());

                for (UUID id : ids) {
                    if (expSizes.get(id) == null)
                        expSizes.put(id, block.length());
                    else
                        expSizes.put(id, expSizes.get(id) + block.length());
                }
            }
        }

        for (int i = 0; i < GRID_CNT; i++) {
            UUID id = grid(i).localNode().id();

            GridCacheAdapter<IgfsBlockKey, byte[]> cache = cache(id);

            int expSize = expSizes.get(id) != null ? expSizes.get(id) : 0;

            assert expSize == cache.igfsDataSpaceUsed();
        }

        // Perform reads which could potentially be non-local.
        byte[] buf = new byte[BLOCK_SIZE];

        for (IgfsFile file : files) {
            for (int i = 0; i < GRID_CNT; i++) {
                int total = 0;

                IgfsInputStream is = igfs(i).open(file.path());

                while (true) {
                    int read = is.read(buf);

                    if (read == -1)
                        break;
                    else
                        total += read;
                }

                assert total == file.length() : "Not enough bytes read: [expected=" + file.length() + ", actual=" +
                    total + ']';

                is.close();
            }
        }

        // Check sizes after read.
        if (cacheMode == PARTITIONED) {
            // No changes since the previous check for co-located cache.
            for (int i = 0; i < GRID_CNT; i++) {
                UUID id = grid(i).localNode().id();

                GridCacheAdapter<IgfsBlockKey, byte[]> cache = cache(id);

                int expSize = expSizes.get(id) != null ? expSizes.get(id) : 0;

                assert expSize == cache.igfsDataSpaceUsed();
            }
        }
        else {
            // All data must exist on each cache.
            int totalSize = 0;

            for (IgfsFile file : files)
                totalSize += file.length();

            for (int i = 0; i < GRID_CNT; i++) {
                UUID id = grid(i).localNode().id();

                GridCacheAdapter<IgfsBlockKey, byte[]> cache = cache(id);

                assertEquals(totalSize, cache.igfsDataSpaceUsed());
            }
        }

        // Delete data and ensure that all counters are 0 now.
        for (IgfsFile file : files) {
            igfs(0).delete(file.path(), false);

            // Await for actual delete to occur.
            for (IgfsBlock block : file.blocks()) {
                for (int i = 0; i < GRID_CNT; i++) {
                    while (localPeek(cache(grid(i).localNode().id()), block.key()) != null)
                        U.sleep(100);
                }
            }
        }

        for (int i = 0; i < GRID_CNT; i++) {
            GridCacheAdapter<IgfsBlockKey, byte[]> cache = cache(grid(i).localNode().id());

            assert 0 == cache.igfsDataSpaceUsed() : "Size counter is not 0: " + cache.igfsDataSpaceUsed();
        }
    }

    /**
     * Ensure that an exception is thrown in case of IGFS oversize.
     *
     * @throws Exception If failed.
     */
    private void checkOversize() throws Exception {
        final long maxSize = 32 * 1024 * 1024;

        memIgfsdDataPlcSetter = new IgniteInClosure<IgniteConfiguration>() {
            @Override public void apply(IgniteConfiguration cfg) {
                String memPlcName = "igfsDataMemPlc";

                cfg.setMemoryConfiguration(new MemoryConfiguration().setMemoryPolicies(
                    new MemoryPolicyConfiguration().setSize(maxSize).setName(memPlcName)));

                FileSystemConfiguration igfsCfg = cfg.getFileSystemConfiguration()[0];

                igfsCfg.getDataCacheConfiguration().setMemoryPolicyName(memPlcName);

                cfg.setCacheConfiguration(new CacheConfiguration().setName("QQQ").setMemoryPolicyName(memPlcName));
            }
        };

        startUp();

        final IgfsPath path = new IgfsPath("/file");

        final int writeChunkSize = (int)(maxSize / 1024);

        // This write is expected to be successful.
        IgfsOutputStream os = igfs(0).create(path, false);
        os.write(chunk(writeChunkSize));
        os.close();

        // This write must be successful as well.
        os = igfs(0).append(path, false);
        os.write(chunk(1));
        os.close();

        // This write must fail w/ exception.
        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgfsOutputStream osErr = igfs(0).append(path, false);

                try {
                    for (int i = 0; i < maxSize / writeChunkSize * GRID_CNT; ++i)
                        osErr.write(chunk(writeChunkSize));

                    osErr.close();

                    return null;
                }
                catch (IOException e) {
                    Throwable e0 = e;

                    while (e0.getCause() != null)
                        e0 = e0.getCause();

                    throw (Exception)e0;
                }
                finally {
                    U.closeQuiet(osErr);
                }
            }
        }, IgniteOutOfMemoryException.class, "Not enough memory allocated");
    }

    /**
     * Ensure that IGFS size is correctly updated in case of preloading.
     *
     * @throws Exception If failed.
     */
    private void checkPreload() throws Exception {
        assert cacheMode == PARTITIONED;

        startUp();

        // Perform writes.
        Collection<IgfsFile> files = write();

        // Check sizes.
        Map<UUID, Integer> expSizes = new HashMap<>(GRID_CNT, 1.0f);

        for (IgfsFile file : files) {
            for (IgfsBlock block : file.blocks()) {
                Collection<UUID> ids = primaryOrBackups(block.key());

                for (UUID id : ids) {
                    if (expSizes.get(id) == null)
                        expSizes.put(id, block.length());
                    else
                        expSizes.put(id, expSizes.get(id) + block.length());
                }
            }
        }

        info("Size map before node start: " + expSizes);

        for (int i = 0; i < GRID_CNT; i++) {
            UUID id = grid(i).localNode().id();

            GridCacheAdapter<IgfsBlockKey, byte[]> cache = cache(id);

            int expSize = expSizes.get(id) != null ? expSizes.get(id) : 0;

            assertEquals(expSize, cache.igfsDataSpaceUsed());
        }

        Ignite g = startGrid(GRID_CNT);

        info("Started grid: " + g.cluster().localNode().id());

        // Wait partitions are evicted.
        awaitPartitionMapExchange();

        // Check sizes again.
        expSizes.clear();

        for (IgfsFile file : files) {
            for (IgfsBlock block : file.blocks()) {
                Collection<UUID> ids = primaryOrBackups(block.key());

                assert !ids.isEmpty();

                for (UUID id : ids) {
                    if (expSizes.get(id) == null)
                        expSizes.put(id, block.length());
                    else
                        expSizes.put(id, expSizes.get(id) + block.length());
                }
            }
        }

        info("Size map after node start: " + expSizes);

        for (int i = 0; i < GRID_CNT - 1; i++) {
            UUID id = grid(i).localNode().id();

            GridCacheAdapter<IgfsBlockKey, byte[]> cache = cache(id);

            int expSize = expSizes.get(id) != null ? expSizes.get(id) : 0;

            assertEquals("For node: " + id, expSize, cache.igfsDataSpaceUsed());
        }
    }

    /**
     * Create data chunk of the given length.
     *
     * @param len Length.
     * @return Data chunk.
     */
    private byte[] chunk(int len) {
        byte[] chunk = new byte[len];

        for (int i = 0; i < len; i++)
            chunk[i] = (byte)i;

        return chunk;
    }

    /**
     * Determine primary and backup node IDs for the given block key.
     *
     * @param key Block key.
     * @return Collection of node IDs.
     */
    private Collection<UUID> primaryOrBackups(IgfsBlockKey key) {
        IgniteEx grid = grid(0);

        Collection<UUID> ids = new HashSet<>();

        for (ClusterNode node : grid.cluster().nodes()) {
            if (grid.affinity(grid.igfsx(IGFS_NAME).configuration().getDataCacheConfiguration().getName())
                .isPrimaryOrBackup(node, key))
                ids.add(node.id());
        }

        return ids;
    }

    /**
     * Get IGFS of a node with the given index.
     *
     * @param idx Node index.
     * @return IGFS.
     * @throws Exception If failed.
     */
    private IgfsImpl igfs(int idx) throws Exception {
        return (IgfsImpl)grid(idx).fileSystem(IGFS_NAME);
    }

    /**
     * Get data cache for the given node ID.
     *
     * @param nodeId Node ID.
     * @return Data cache.
     */
    private GridCacheAdapter<IgfsBlockKey, byte[]> cache(UUID nodeId) {
        IgniteEx g = (IgniteEx)G.ignite(nodeId);
        return (GridCacheAdapter<IgfsBlockKey, byte[]>)g.cachex(g.igfsx(IGFS_NAME).configuration()
            .getDataCacheConfiguration().getName()).<IgfsBlockKey, byte[]>cache();
    }

    /**
     * Perform write of the files.
     *
     * @return Collection of written file descriptors.
     * @throws Exception If failed.
     */
    private Collection<IgfsFile> write() throws Exception {
        Collection<IgfsFile> res = new HashSet<>(FILES_CNT, 1.0f);

        ThreadLocalRandom8 rand = ThreadLocalRandom8.current();

        for (int i = 0; i < FILES_CNT; i++) {
            // Create empty file locally.
            IgfsPath path = new IgfsPath("/file-" + i);

            igfs(0).create(path, false).close();

            IgfsMetaManager meta = igfs(0).context().meta();

            IgniteUuid fileId = meta.fileId(path);

            // Calculate file blocks.
            int fileSize = rand.nextInt(MAX_FILE_SIZE);

            int fullBlocks = fileSize / BLOCK_SIZE;
            int remainderSize = fileSize % BLOCK_SIZE;

            Collection<IgfsBlock> blocks = new ArrayList<>(fullBlocks + remainderSize > 0 ? 1 : 0);

            for (int j = 0; j < fullBlocks; j++)
                blocks.add(new IgfsBlock(new IgfsBlockKey(fileId, null, true, j), BLOCK_SIZE));

            if (remainderSize > 0)
                blocks.add(new IgfsBlock(new IgfsBlockKey(fileId, null, true, fullBlocks), remainderSize));

            IgfsFile file = new IgfsFile(path, fileSize, blocks);

            // Actual write.
            for (IgfsBlock block : blocks) {
                IgfsOutputStream os = igfs(0).append(path, false);

                os.write(chunk(block.length()));

                os.close();
            }

            // Add written file to the result set.
            res.add(file);
        }

        return res;
    }

    /** A file written to the file system. */
    private static class IgfsFile {
        /** Path to the file, */
        private final IgfsPath path;

        /** File length. */
        private final int len;

        /** Blocks with their corresponding locations. */
        private final Collection<IgfsBlock> blocks;

        /**
         * Constructor.
         *
         * @param path Path.
         * @param len Length.
         * @param blocks Blocks.
         */
        private IgfsFile(IgfsPath path, int len, Collection<IgfsBlock> blocks) {
            this.path = path;
            this.len = len;
            this.blocks = blocks;
        }

        /** @return Path. */
        IgfsPath path() {
            return path;
        }

        /** @return Length. */
        int length() {
            return len;
        }

        /** @return Blocks. */
        Collection<IgfsBlock> blocks() {
            return blocks;
        }
    }

    /** Block written to the file system. */
    private static class IgfsBlock {
        /** Block key. */
        private final IgfsBlockKey key;

        /** Block length. */
        private final int len;

        /**
         * Constructor.
         *
         * @param key Block key.
         * @param len Block length.
         */
        private IgfsBlock(IgfsBlockKey key, int len) {
            this.key = key;
            this.len = len;
        }

        /** @return Block key. */
        private IgfsBlockKey key() {
            return key;
        }

        /** @return Block length. */
        private int length() {
            return len;
        }
    }
}