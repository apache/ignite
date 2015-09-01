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
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.IgfsOutputStream;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.internal.processors.igfs.IgfsAbstractSelfTest.awaitFileClose;
import static org.apache.ignite.internal.processors.igfs.IgfsAbstractSelfTest.checkExist;
import static org.apache.ignite.internal.processors.igfs.IgfsAbstractSelfTest.checkFileContent;
import static org.apache.ignite.internal.processors.igfs.IgfsAbstractSelfTest.clear;
import static org.apache.ignite.internal.processors.igfs.IgfsAbstractSelfTest.createFile;
import static org.apache.ignite.internal.processors.igfs.IgfsAbstractSelfTest.paths;
import static org.apache.ignite.internal.processors.igfs.IgfsAbstractSelfTest.writeFileChunks;

/**
 * Tests IGFS behavioral guarantees if some nodes on the cluster are synchronously or asynchronously stopped.
 * The operations to check are read, write or both.
 */
public class IgfsBackupFailoverSelfTest extends IgfsCommonAbstractTest {
    /** Directory. */
    protected static final IgfsPath DIR = new IgfsPath("/dir");

    /** Sub-directory. */
    protected static final IgfsPath SUBDIR = new IgfsPath(DIR, "subdir");

    /** Number of Ignite nodes used in failover tests. */
    protected final int numIgfsNodes = 5;

    /** Number of backup copies of data (aka replication). */
    protected final int numBackups = numIgfsNodes - 1;

    /** */
    private final int fileSize = 16 * 1024;

    /** */
    private final int files = 500;

    /** File block size. Use Very small blocks to ensure uniform data distribution among the nodes.. */
    protected int igfsBlockSize = 31;

    /** Affinity group size (see IgfsGroupDataBlocksKeyMapper). */
    protected int affGrpSize = 1;

    /** IGFS mode. */
    protected IgfsMode igfsMode = IgfsMode.PRIMARY;

    /** Node data structures. */
    protected NodeFsData[] nodeDatas;

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

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

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
     * Checks correct data read *after* N-1 nodes are stopped.
     *
     * @throws Exception On error.
     */
    public void testReadFailoverAfterStopMultipleNodes() throws Exception {
        final IgfsImpl igfs0 = nodeDatas[0].igfsImpl;

        clear(igfs0);

        IgfsAbstractSelfTest.create(igfs0, paths(DIR, SUBDIR), null);

        // Create files through the 0th node:
        for (int f=0; f<files; f++) {
            final byte[] data = createChunk(fileSize, f);

            createFile(igfs0, filePath(f), true, -1/*block size unused*/, data);
        }

        // Check files:
        for (int f=0; f<files; f++) {
            IgfsPath path = filePath(f);
            byte[] data = createChunk(fileSize, f);

            // Check through 0th node:
            checkExist(igfs0, path);

            checkFileContent(igfs0, path, data);

            // Check the same file through other nodes:
            for (int n=1; n<numIgfsNodes; n++) {
                checkExist(nodeDatas[n].igfsImpl, path);

                checkFileContent(nodeDatas[n].igfsImpl, path, data);
            }
        }

        // Now stop all the nodes but the 1st:
        for (int n=1; n<numIgfsNodes; n++)
            stopGrid(n);

        // Check files again:
        for (int f=0; f<files; f++) {
            IgfsPath path = filePath(f);

            byte[] data = createChunk(fileSize, f);

            // Check through 0th node:
            checkExist(igfs0, path);

            checkFileContent(igfs0, path, data);
        }
    }

    /**
     * Checks correct data read *while* N-1 nodes are being concurrently stopped.
     *
     * @throws Exception On error.
     */
    public void testReadFailoverWhileStoppingMultipleNodes() throws Exception {
        final IgfsImpl igfs0 = nodeDatas[0].igfsImpl;

        clear(igfs0);

        IgfsAbstractSelfTest.create(igfs0, paths(DIR, SUBDIR), null);

        // Create files:
        for (int f = 0; f < files; f++) {
            final byte[] data = createChunk(fileSize, f);

            createFile(igfs0, filePath(f), true, -1/*block size unused*/, data);
        }

        // Check files:
        for (int f = 0; f < files; f++) {
            IgfsPath path = filePath(f);
            byte[] data = createChunk(fileSize, f);

            // Check through 1st node:
            checkExist(igfs0, path);

            checkFileContent(igfs0, path, data);

            // Check the same file through other nodes:
            for (int n = 1; n < numIgfsNodes; n++) {
                checkExist(nodeDatas[n].igfsImpl, path);

                checkFileContent(nodeDatas[n].igfsImpl, path, data);
            }
        }

        final AtomicBoolean stop = new AtomicBoolean();

        GridTestUtils.runMultiThreadedAsync(new Callable() {
            @Override public Object call() throws Exception {
                Thread.sleep(1_000); // Some delay to ensure read is in progress.

                // Now stop all the nodes but the 1st:
                for (int n = 1; n < numIgfsNodes; n++) {
                    stopGrid(n);

                    X.println("grid " + n + " stopped.");
                }

                Thread.sleep(1_000);

                stop.set(true);

                return null;
            }
        }, 1, "igfs-node-stopper");

        // Read the files repeatedly, while the nodes are being stopped:
        while (!stop.get()) {
            // Check files while the nodes are being stopped:
            for (int f = 0; f < files; f++) {
                IgfsPath path = filePath(f);

                byte[] data = createChunk(fileSize, f);

                // Check through 1st node:
                checkExist(igfs0, path);

                checkFileContent(igfs0, path, data);
            }
        }
    }

    /**
     * Checks possibility to append the data to files *after* N-1 nodes are stopped.
     * First, some data written to files.
     * After that N-1 nodes are stopped.
     * Then data are attempted to append to the streams opened before the nodes stop.
     * If failed, the streams are attempted to reopen and the files are attempted to append.
     * After that the read operation is performed to check data correctness.
     *
     * The test is temporarily disabled due to issues .... .
     *
     * @throws Exception On error.
     */
    public void testWriteFailoverAfterStopMultipleNodes() throws Exception {
        final IgfsImpl igfs0 = nodeDatas[0].igfsImpl;

        clear(igfs0);

        IgfsAbstractSelfTest.create(igfs0, paths(DIR, SUBDIR), null);

        final IgfsOutputStream[] outStreams = new IgfsOutputStream[files];

        // Create files:
        for (int f = 0; f < files; f++) {
            final byte[] data = createChunk(fileSize, f);

            IgfsOutputStream os = null;

            try {
                os = igfs0.create(filePath(f), 256, true, null, 0, -1, null);

                assert os != null;

                writeFileChunks(os, data);
            }
            finally {
                if (os != null)
                    os.flush();
            }

            outStreams[f] = os;

            X.println("write #1 completed: " + f);
        }

        // Now stop all the nodes but the 1st:
        for (int n = 1; n < numIgfsNodes; n++) {
            stopGrid(n);

            X.println("#### grid " + n + " stopped.");
        }

        // Create files:
        for (int f0 = 0; f0 < files; f0++) {
            final IgfsOutputStream os = outStreams[f0];

            assert os != null;

            final int f = f0;

            int att = doWithRetries(1, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    IgfsOutputStream ios = os;

                    try {
                        writeChunks0(igfs0, ios, f);
                    }
                    catch (IOException ioe) {
                        log().warning("Attempt to append the data to existing stream failed: ", ioe);

                        ios = igfs0.append(filePath(f), false);

                        assert ios != null;

                        writeChunks0(igfs0, ios, f);
                    }

                    return null;
                }
            });

            assert att == 1;

            X.println("write #2 completed: " + f0 + " in " + att + " attempts.");
        }

        // Check files:
        for (int f = 0; f < files; f++) {
            IgfsPath path = filePath(f);

            byte[] data = createChunk(fileSize, f);

            // Check through 1st node:
            checkExist(igfs0, path);

            assertEquals("File length mismatch.", data.length * 2, igfs0.size(path));

            checkFileContent(igfs0, path, data, data);

            X.println("Read test completed: " + f);
        }
    }

    /**
     *
     * @throws Exception
     */
    public void testWriteFailoverWhileStoppingMultipleNodes() throws Exception {
        final IgfsImpl igfs0 = nodeDatas[0].igfsImpl;

        clear(igfs0);

        IgfsAbstractSelfTest.create(igfs0, paths(DIR, SUBDIR), null);

        final IgfsOutputStream[] outStreams = new IgfsOutputStream[files];

        // Create files:
        for (int f = 0; f < files; f++) {
            final byte[] data = createChunk(fileSize, f);

            IgfsOutputStream os = null;

            try {
                os = igfs0.create(filePath(f), 256, true, null, 0, -1, null);

                assert os != null;

                writeFileChunks(os, data);
            }
            finally {
                if (os != null)
                    os.flush();
            }

            outStreams[f] = os;

            X.println("write #1 completed: " + f);
        }

        final AtomicBoolean stop = new AtomicBoolean();

        GridTestUtils.runMultiThreadedAsync(new Callable() {
            @Override public Object call() throws Exception {
                Thread.sleep(10_000); // Some delay to ensure read is in progress.

                // Now stop all the nodes but the 1st:
                for (int n = 1; n < numIgfsNodes; n++) {
                    stopGrid(n);

                    X.println("#### grid " + n + " stopped.");
                }

                //Thread.sleep(10_000);

                stop.set(true);

                return null;
            }
        }, 1, "igfs-node-stopper");

        // Write #2:
        for (int f0 = 0; f0 < files; f0++) {
            final IgfsOutputStream os = outStreams[f0];

            assert os != null;

            final int f = f0;

            int att = doWithRetries(1, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    IgfsOutputStream ios = os;

                    try {
                        writeChunks0(igfs0, ios, f);
                    }
                    catch (IOException ioe) {
                        log().warning("Attempt to append the data to existing stream failed: ", ioe);

                        ios = igfs0.append(filePath(f), false);

                        assert ios != null;

                        writeChunks0(igfs0, ios, f);
                    }

                    return null;
                }
            });

            assert att == 1;

            X.println("write #2 completed: " + f0 + " in " + att + " attempts.");
        }

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return stop.get();
            }
        }, 25_000);

        // Check files:
        for (int f = 0; f < files; f++) {
            IgfsPath path = filePath(f);

            byte[] data = createChunk(fileSize, f);

            // Check through 1st node:
            checkExist(igfs0, path);

            assertEquals("File length mismatch.", data.length * 2, igfs0.size(path));

            checkFileContent(igfs0, path, data, data);

            X.println("Read test completed: " + f);
        }
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

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 20 * 60 * 1000;
    }
}