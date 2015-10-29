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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.IgfsOutputStream;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.internal.processors.igfs.IgfsAbstractSelfTest.awaitFileClose;
import static org.apache.ignite.internal.processors.igfs.IgfsAbstractSelfTest.checkExist;
import static org.apache.ignite.internal.processors.igfs.IgfsAbstractSelfTest.checkFileContent;
import static org.apache.ignite.internal.processors.igfs.IgfsAbstractSelfTest.clear;
import static org.apache.ignite.internal.processors.igfs.IgfsAbstractSelfTest.getDataCache;
import static org.apache.ignite.internal.processors.igfs.IgfsAbstractSelfTest.getMetaCache;
import static org.apache.ignite.internal.processors.igfs.IgfsAbstractSelfTest.paths;
import static org.apache.ignite.internal.processors.igfs.IgfsAbstractSelfTest.writeFileChunks;

/**
 * Tests IGFS behavioral guarantees if some nodes on the cluster are synchronously or asynchronously stopped.
 * The operations to check are read, write or both.
 */
public class IgfsWriteFailoverCleanupSelfTest extends IgfsCommonAbstractTest {
    /**  */
    protected static final Map<String,String> PREF_LOCAL_WRITES_FILE_PROPS
        = Collections.unmodifiableMap(
            F.asMap(IgfsEx.PROP_PREFER_LOCAL_WRITES, "true"));

    /** Directory. */
    protected static final IgfsPath DIR = new IgfsPath("/dir");

    /** Sub-directory. */
    protected static final IgfsPath SUBDIR = new IgfsPath(DIR, "subdir");

    /** Number of Ignite nodes used in failover tests. */
    protected final int numIgfsNodes = 5;

    /** Number of backup copies of data (aka replication). */
    protected int numBackups = 0;

    /** Number of files used in test. */
    private final int files = 51;

    /** File block size. Use Very small blocks to ensure uniform data distribution among the nodes. */
    protected int igfsBlockSize = 31;

    /** If fragmentizer enabled. */
    protected boolean fragmentizerEnabled = false;

    /** */
    private final int fileSize = 77 * igfsBlockSize + 17;

    /** Affinity group size (see IgfsGroupDataBlocksKeyMapper). */
    protected int affGrpSize = 1;

    /** IGFS mode. */
    protected IgfsMode igfsMode = IgfsMode.PRIMARY;

    /** Node data structures. */
    protected NodeFsData[] nodeDatas;

    /** Properties for created files. */
    protected Map<String,String> fileProps;

    /**
     * Structure to hold Ignite IGFS node data.
     */
    static class NodeFsData {
        /**  */
        int idx;

        /**  */
        Ignite ignite;

        /**  */
        IgfsImpl igfsImpl;
    }

    /**
     * Performs init.
     *
     * @throws Exception On error.
     */
    protected void startUp() throws Exception {
        nodeDatas = new NodeFsData[numIgfsNodes];

        for (int i = 0; i<numIgfsNodes; i++) {
            NodeFsData data = new NodeFsData();

            data.idx = i;

            data.ignite = startGridWithIgfs(getTestGridName(i), igfsMode, null);

            data.igfsImpl = (IgfsImpl) data.ignite.fileSystem("igfs");

            nodeDatas[i] = data;
        }

        // Ensure all the nodes are started and discovered each other:
        checkTopology(numIgfsNodes);
    }

    /**
     * Start grid with IGFS.
     *
     * @param gridName Grid name.
     * @param mode IGFS mode.
     * @param secondaryFs Secondary file system (optional).
     * @return Started grid instance.
     * @throws Exception If failed.
     */
    protected Ignite startGridWithIgfs(String gridName, IgfsMode mode, @Nullable IgfsSecondaryFileSystem secondaryFs)
        throws Exception {
        final FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setDataCacheName("dataCache");
        igfsCfg.setMetaCacheName("metaCache");
        igfsCfg.setName("igfs");
        igfsCfg.setBlockSize(igfsBlockSize);
        igfsCfg.setDefaultMode(mode);
        igfsCfg.setSecondaryFileSystem(secondaryFs);
        igfsCfg.setFragmentizerEnabled(fragmentizerEnabled);

        CacheConfiguration<?,?> dataCacheCfg = defaultCacheConfiguration();

        dataCacheCfg.setName("dataCache");
        dataCacheCfg.setCacheMode(PARTITIONED);
        dataCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        dataCacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(affGrpSize));
        dataCacheCfg.setBackups(numBackups);
        dataCacheCfg.setAtomicityMode(TRANSACTIONAL);

        CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setName("metaCache");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName(gridName);

        cfg.setCacheConfiguration(dataCacheCfg, metaCacheCfg);
        cfg.setFileSystemConfiguration(igfsCfg);

        cfg.setLocalHost("127.0.0.1");

        return startGrid(gridName, cfg);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        G.stopAll(false);

        Arrays.fill(nodeDatas, null);
    }

    /**
     * Creates a chunk of data.
     *
     * @param len The chunk length
     * @param j index to scramble into the data.
     * @return The chunk.
     */
    static byte[] createChunk(int len, int j) {
        byte[] bb = new byte[len];

        for (int i = 0; i < bb.length; i++)
            bb[i] = (byte)(i ^ j);

        return bb;
    }

    /**
     * Composes the path of the file.
     */
    private IgfsPath filePath(int j) {
        return new IgfsPath(SUBDIR, "file" + j);
    }

    /**
     *
     * @throws Exception
     */
    public void testCleanupAfterStoppingMultipleNodesCreate() throws Exception {
        // In this test we have *no* backups, and crash all the nodes but one,
        // so likely all the files are destroyed.
        // But there is nothing bad in that since the files are overwritten with "create(overwrite=true)" operation:
        // old files should be unlocked and moved to TRASH, while new files should be created.
        numBackups = 0;

        fragmentizerEnabled = false;

        fileProps = null;

        createTest0();
    }

    /**
     *
     * @throws Exception
     */
    public void testCleanupAfterStoppingMultipleNodesCreateSpecialAffinity() throws Exception {
        // In this test we have *no* backups, and crash all the nodes but one,
        // so likely all the files are destroyed.
        // But there is nothing bad in that since the files are overwritten with "create(overwrite=true)" operation:
        // old files should be unlocked and moved to TRASH, while new files should be created.
        numBackups = 0;

        fragmentizerEnabled = true;

        fileProps = PREF_LOCAL_WRITES_FILE_PROPS;

        createTest0();
    }

    /**
     * Create test implementation.
     *
     * @throws Exception On error.
     */
    protected void createTest0() throws Exception {
        startUp();

        final IgfsImpl igfs0 = nodeDatas[0].igfsImpl;

        clear(igfs0);

        IgfsAbstractSelfTest.create(igfs0, paths(DIR, SUBDIR), null);

        // Create files:
        for (int f = 0; f < files; f++) {
            final byte[] data = createChunk(fileSize, f);

            IgfsOutputStream os = null;

            try {
                os = igfs0.create(filePath(f), 256, true, null, 0, -1, null);

                assert os != null;

                writeFileChunks(os, data);

                os.flush();
            }
            catch (Exception e) {
                // Ignore the exception there because there are no backups.
                X.println("" + e);
            }
            finally {
                U.closeQuiet(os); // Also ignore exceptions there.
            }
        }

        boolean ok = checkIgfsIntegrity(igfs0);

        assert ok;

        final AtomicBoolean nodesStopped = new AtomicBoolean();

        final AtomicInteger cnt = new AtomicInteger();

        GridTestUtils.runMultiThreadedAsync(new Callable() {
            @Override public Object call() throws Exception {
                assert GridTestUtils.waitForCondition(new GridAbsPredicate() {
                    @Override public boolean apply() {
                        return cnt.get() >= 10;
                    }
                }, 60_000);

                // Now stop all the nodes but the 1st:
                for (int n = 0; n < numIgfsNodes - 1; n++) {
                    stopGrid(n);

                    X.println("Node " + n + " stopped.");
                }

                boolean set = nodesStopped.compareAndSet(false, true);

                assert set;

                return null;
            }
        }, 1, "igfs-node-stopper");

        // Create files:
        for (int f = 0; f < files; f++) {
            final byte[] data = createChunk(fileSize, f);

            IgfsOutputStream os = null;

            try {
                os = igfs0.append(filePath(f), false);

                assert os != null;

                writeFileChunks(os, data);

                os.flush();
            }
            catch (Exception e) {
                // Ignore the exception there because there are no backups.
                X.println("" + e);
            }
            finally {
                U.closeQuiet(os); // Also ignore exceptions there.
            }

            cnt.incrementAndGet();

            // Wait all nodes are stopped, then continue:
            if (cnt.get() == (3 * files) / 4) {
                X.println("Waiting all nodes to stop..");

                assert GridTestUtils.waitForCondition(new GridAbsPredicate() {
                    @Override public boolean apply() {
                        return nodesStopped.get();
                    }
                }, 60_000);
            }
        }

        assert nodesStopped.get(); // it must have happened.

        final IgfsImpl igfs1 = nodeDatas[numIgfsNodes - 1].igfsImpl;

        // Write #2:
        for (int f0 = 0; f0 < files; f0++) {
            final int f = f0;

            int att = doWithRetries(3, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    // Any way should be able to write over the files , even if they are corrupted:
                    IgfsOutputStream ios = igfs1.create(filePath(f), true/*overwrite*/);

                    assert ios != null;

                    writeChunks0(igfs1, ios, f);

                    return null;
                }
            });

            if (att > 1)
                X.println("write #2 completed: " + f0 + " in " + att + " attempts.");
        }

        // Check files:
        for (int f = 0; f < files; f++) {
            IgfsPath path = filePath(f);

            byte[] data = createChunk(fileSize, f);

            // Check through 1st node:
            checkExist(igfs1, path);

            assertEquals("File length mismatch.", data.length, igfs1.size(path));

            checkFileContent(igfs1, path, data, data);
        }

        igfs1.awaitDeletesAsync().get();

        X.println("Deletions completed.");

        ok = checkIgfsIntegrity(igfs1);

        assert ok;
    }

    /**
     *
     * @throws Exception
     */
    public void testCleanupAfterStoppingMultipleNodesAppend() throws Exception {
        // In this test we have a backup for each file block, and stop only one (the 0-th) node,
        // so no data should be lost.
        numBackups = 3; // Normally should be 1, but for some reason it works only for 3 and above.

        fragmentizerEnabled = false;

        fileProps = null;

        appendTest0();
    }

    /**
     *
     * @throws Exception
     */
    public void testCleanupAfterStoppingMultipleNodesAppendSpecialAffinity() throws Exception {
        // In this test we have a backup for each file block, and stop only one (the 0-th) node,
        // so no data should be lost.
        numBackups = 3; // Normally should be 1, but for some reason it works only for 3 and above.

        fragmentizerEnabled = true;

        fileProps = PREF_LOCAL_WRITES_FILE_PROPS;

        appendTest0();
    }

    /**
     * Append test implementation.
     *
     * @throws Exception On error.
     */
    protected void appendTest0() throws Exception {
        startUp();

        final IgfsImpl igfs0 = nodeDatas[0].igfsImpl;

        clear(igfs0);

        IgfsAbstractSelfTest.create(igfs0, paths(DIR, SUBDIR), null);

        // Create files:
        for (int f = 0; f < files; f++) {
            final byte[] data = createChunk(fileSize, f);

            IgfsOutputStream os = null;

            try {
                os = igfs0.create(filePath(f), 256, true, null, 0, -1, fileProps);

                assert os != null;

                writeFileChunks(os, data);

                os.flush();
            }
            catch (Exception e) {
                // Ignore the exception there because there are no backups.
                X.println("" + e);
            }
            finally {
                U.closeQuiet(os); // Also ignore exceptions there.
            }
        }

        assert checkIgfsIntegrity(igfs0);

        final AtomicBoolean nodesStopped = new AtomicBoolean();

        final AtomicInteger cnt = new AtomicInteger();

        GridTestUtils.runMultiThreadedAsync(new Callable() {
            @Override public Object call() throws Exception {
                assert GridTestUtils.waitForCondition(new GridAbsPredicate() {
                    @Override public boolean apply() {
                        return cnt.get() >= 10;
                    }
                }, 60_000);

                // Now stop *only* the 1st node:
                stopGrid(0);

                X.println("#### Node 0 stopped.");

                boolean set = nodesStopped.compareAndSet(false, true);

                assert set;

                return null;
            }
        }, 1, "igfs-node-stopper");

        // Create files:
        for (int f = 0; f < files; f++) {
            final byte[] data = createChunk(fileSize, f);

            IgfsOutputStream os = null;

            try {
                os = igfs0.append(filePath(f), false);

                assert os != null;

                writeFileChunks(os, data);

                os.flush();
            }
            catch (Exception e) {
                // Ignore the exception there because there are no backups.
                X.println("" + e);
            }
            finally {
                U.closeQuiet(os); // Also ignore exceptions there.
            }

            cnt.incrementAndGet();

            // Wait all nodes are stopped, then continue:
            if (cnt.get() == (3 * files) / 4) {
                X.println("Waiting all nodes to stop..");

                assert GridTestUtils.waitForCondition(new GridAbsPredicate() {
                    @Override public boolean apply() {
                        return nodesStopped.get();
                    }
                }, 60_000);
            }
        }

        assert nodesStopped.get(); // it must have happened.

        final IgfsImpl igfs1 = nodeDatas[numIgfsNodes - 1].igfsImpl;

        // Write #2: append:
        for (int f0 = 0; f0 < files; f0++) {
            final int f = f0;

            int att = doWithRetries(3, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    // Any way should be able to write over the files , even if they are corrupted:
                    IgfsOutputStream ios = igfs1.append(filePath(f), false/*create*/);

                    assert ios != null;

                    writeChunks0(igfs1, ios, f);

                    return null;
                }
            });

            if (att > 1)
                X.println("write #2 completed: " + f0 + " in " + att + " attempts.");
        }

        X.println("================ Finished append after node stop.");

        igfs1.awaitDeletesAsync().get();

        X.println("Deletions completed.");

        boolean ok = checkIgfsIntegrity(igfs1);

        assert ok;
    }

    /**
     * Checks integrity of IGFS: several audit rules on Meta and Data caches.
     * Note: currently the IGFS content expected to be known and equal to
     * pre-defined set of files.
     *
     * @param igfs The IGFS to check.
     * @return If the check is passed.
     * @throws IgniteCheckedException On error.
     */
    private boolean checkIgfsIntegrity(IgfsImpl igfs) throws IgniteCheckedException {
        assert igfs != null;

        GridCacheAdapter<IgfsBlockKey, byte[]> dataCache = getDataCache(igfs);

        assert dataCache != null;

        boolean ok = true;

        // Compose data map grouping all the data cache entries by their file key:
        Map<IgniteUuid, SortedMap<IgfsBlockKey, byte[]>> dataMap = new HashMap<>();

        IgfsBlockKey key;

        for (Cache.Entry<IgfsBlockKey, byte[]> e: dataCache.entrySet()) {
            key = e.getKey();

            IgniteUuid fileId = key.getFileId();

            SortedMap<IgfsBlockKey, byte[]> blockMap = dataMap.get(fileId);

            if (blockMap == null) {
                blockMap = new TreeMap<>(new Comparator<IgfsBlockKey>() {
                    @Override public int compare(IgfsBlockKey k1, IgfsBlockKey k2) {
                        long diff = k1.getBlockId() - k2.getBlockId();

                        if (diff == 0)
                            return 0;

                        return diff > 0 ? 1 : -1;
                    }
                });

                dataMap.put(fileId, blockMap);
            }

            blockMap.put(key, e.getValue());
        }

        X.println("Files found in data cache: " + dataMap.size());

        GridCacheAdapter<IgniteUuid, IgfsFileInfo> metaCache = getMetaCache(igfs);

        IgfsMetaManager meta = igfs.getMeta();

        Map<IgniteUuid, IgfsPath> id2path = new HashMap<>();

        for (int i=0; i< files; i++) {
            IgfsPath path = filePath(i);

            IgniteUuid id = meta.fileId(path);

            assert id != null;

            id2path.put(id, path);
        }

        assert id2path.size() == files;

        IgniteUuid fileId;

        IgfsFileInfo fi;

        for (Cache.Entry<IgniteUuid, IgfsFileInfo> e: metaCache.entrySet()) {
            fileId = e.getKey();
            fi = e.getValue();

            ok &= checkFile(fileId, fi, dataMap.remove(fileId));

            if (fi.isFile() && id2path.get(fileId) == null) {
                X.println(" ! File " + fileId + " not present in path map");

                ok = false;
            }
        }

        if (dataMap.isEmpty())
            X.println("All data cache files are present in meta cache.");
        else {
            ok = false;

            X.println("! Lost entries (present only in data cache):");

            for (Map.Entry<IgniteUuid, SortedMap<IgfsBlockKey, byte[]>> e : dataMap.entrySet())
                X.println("   k = " + e.getKey() + ", map = " + e.getValue().keySet());
        }

        return ok;
    }

    /**
     * Checks one file integrity.
     * @param fileId The id.
     * @param fi The FileInfo.
     * @param blockMap The block map of this file.
     * @return If the file is okay.
     */
    private boolean checkFile(IgniteUuid fileId, IgfsFileInfo fi, SortedMap<IgfsBlockKey, byte[]> blockMap) {
        if (!fi.isFile())
            return true; // Ignore directories

        boolean ok = true;

        if (fi.lockId() != null) {
            X.println(fileId + " ! lockId is present: " + fi.lockId());

            ok = false;
        }

        long infoDeclaredLen = fi.length();

        long reserved = fi.reservedDelta();

        assert reserved == 0;

        long expBlockId = 0;

        long actualLen = 0;

        for (Map.Entry <IgfsBlockKey, byte[]> e : blockMap.entrySet()) {
            IgfsBlockKey key = e.getKey();

            byte[] val = e.getValue();

            if (key.getBlockId() != expBlockId) {
                X.println(fileId + " ! Expected blockId: " + expBlockId + " != actual block id: " + key.getBlockId());

                ok = false;
            }

            assert key.getFileId().equals(fileId);

            actualLen += val.length;

            expBlockId++;
        }

        if (infoDeclaredLen != actualLen) {
            X.println(fileId + " ! Length declared in file info " + infoDeclaredLen + " != actual len (block sum) = "
                + actualLen);

            ok = false;
        }

        if (actualLen % fileSize != 0 ) {
            X.println(" ! Non zero remainder: " + actualLen % fileSize);

            ok = false;
        }

        long x = actualLen / fileSize;

        if (x == 0 || x > 3) {
            X.println(" ! Unexpected fraction value: " + x);

            ok = false;
        }

        return ok;
    }

    /**
     * Writes data to the file of the specified index and closes the output stream.
     *
     * @param igfs0 IGFS.
     * @param ios The output stream
     * @param fileIdx Th eindex of the file.
     * @throws IOException On error.
     */
    void writeChunks0(IgfsEx igfs0, IgfsOutputStream ios, int fileIdx) throws IOException {
        try {
            byte[] data = createChunk(fileSize, fileIdx);

            writeFileChunks(ios, data);
        }
        finally {
            ios.flush();

            U.closeQuiet(ios);

            awaitFileClose(igfs0.asSecondary(), filePath(fileIdx));
        }
    }

    /**
     * Performs an operation with retries.
     *
     * @param attempts The maximum number of attempts.
     * @param clo The closure to execute.
     * @throws Exception On error.
     */
    protected static int doWithRetries(int attempts, Callable<Void> clo) throws Exception {
        int attemptCnt = 0;

        while (true) {
            try {
                attemptCnt++;

                clo.call();

                return attemptCnt;
            }
            catch (Exception e) {
                if (attemptCnt >= attempts)
                    throw e;
                else
                    X.println("Failed to execute closure in " + attempts + " attempts.");
            }
        }
    }
}