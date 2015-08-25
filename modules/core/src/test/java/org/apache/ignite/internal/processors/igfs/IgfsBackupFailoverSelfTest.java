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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.igfs.*;
import org.apache.ignite.igfs.secondary.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.util.worker.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.internal.processors.igfs.IgfsAbstractSelfTest.*;

/**
 *
 */
public class IgfsBackupFailoverSelfTest extends IgfsCommonAbstractTest {
    /** */
    protected static final IgfsPath DIR = new IgfsPath("/dir");

    /** Sub-directory. */
    protected static final IgfsPath SUBDIR = new IgfsPath(DIR, "subdir");

    /** Amount of blocks to prefetch. */
    protected static final int DFLT_PREFETCH_BLOCKS = 1;

    /** Amount of sequential block reads before prefetch is triggered. */
    protected static final int DFLT_SEQ_READS_BEFORE_PREFETCH = 2;

    /** Number of Ignite nodes. */
    protected int numIgfsNodes = 2;

    /** Number of backup copies of data (aka replication). */
    protected int numBackups = numIgfsNodes - 1;

    /** File block size. */
    protected int igfsBlockSize = 31; // Use Very small blocks.

    /**  */
    protected int affGrpSize = 1;

    /**  */
    protected IgfsMode igfsMode = IgfsMode.PRIMARY;

    /** Memory mode. */
    protected final CacheMemoryMode memoryMode;

    /**  */
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

    /**
     * Constructor.
     */
    public IgfsBackupFailoverSelfTest() {

        memoryMode = CacheMemoryMode.ONHEAP_TIERED;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        nodeDatas = new NodeFsData[numIgfsNodes];

        for (int i = 0; i<numIgfsNodes; i++) {
            NodeFsData data = new NodeFsData();

            data.idx = i;

            IgfsIpcEndpointConfiguration igfsIpcCfg = createIgfsRestConfig(10500 + i);

            data.ignite = startGridWithIgfs(getTestGridName(i), "igfs", igfsMode, null, igfsIpcCfg);

            data.igfsImpl = (IgfsImpl) data.ignite.fileSystem("igfs");

            nodeDatas[i] = data;
        }

        // Ensure all the nodes are started and discovered each other.
        checkTopology(numIgfsNodes);
    }

    /**
     *
     * @param port
     * @return
     */
    protected IgfsIpcEndpointConfiguration createIgfsRestConfig(int port) {
        IgfsIpcEndpointConfiguration cfg = new IgfsIpcEndpointConfiguration();

        cfg.setType(IgfsIpcEndpointType.TCP);
        cfg.setPort(port);

        return cfg;
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
        final FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setDataCacheName("dataCache");
        igfsCfg.setMetaCacheName("metaCache");
        igfsCfg.setName(igfsName);
        igfsCfg.setBlockSize(igfsBlockSize);
        igfsCfg.setDefaultMode(mode);
        igfsCfg.setIpcEndpointConfiguration(restCfg);
        igfsCfg.setSecondaryFileSystem(secondaryFs);
        igfsCfg.setPrefetchBlocks(DFLT_PREFETCH_BLOCKS);
        igfsCfg.setSequentialReadsBeforePrefetch(DFLT_SEQ_READS_BEFORE_PREFETCH);

        CacheConfiguration dataCacheCfg = defaultCacheConfiguration();

        dataCacheCfg.setName("dataCache");
        dataCacheCfg.setCacheMode(PARTITIONED);
        dataCacheCfg.setNearConfiguration(null);
        dataCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        dataCacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(affGrpSize));
        dataCacheCfg.setBackups(numBackups);
        dataCacheCfg.setAtomicityMode(TRANSACTIONAL);
        dataCacheCfg.setMemoryMode(memoryMode);
        dataCacheCfg.setOffHeapMaxMemory(0);

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
     *
     * @param length
     * @return
     */
    static byte[] createChunk(int length, int j) {
        byte[] chunk = new byte[length];

        for (int i = 0; i < chunk.length; i++)
            chunk[i] = (byte)(i ^ j);

        return chunk;
    }

    /** */
    private IgfsPath filePath(int j) {
        return new IgfsPath(SUBDIR, "file" + j);
    }

    /**
     *
     * @throws Exception
     */
    public void testFailoverMultipleNodes() throws Exception {
        final IgfsImpl igfs0 = nodeDatas[0].igfsImpl;

        clear(igfs0);

        IgfsAbstractSelfTest.create(igfs0, paths(DIR, SUBDIR), null);

        final int files = 500;

        final int fileSize = 16 * 1024;

        // Create files:
        for (int f=0; f<files; f++) {
            final byte[] data = createChunk(fileSize, f);

            createFile(igfs0, filePath(f), true, -1/*block size unused*/, data);
        }

        // Check files:
        for (int f=0; f<files; f++) {
            IgfsPath path = filePath(f);
            byte[] data = createChunk(fileSize, f);

            // Check through 1st node:
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

            // Check through 1st node:
            checkExist(igfs0, path);
            checkFileContent(igfs0, path, data);
        }
    }

    /**
     *
     * @throws Exception
     */
    public void testFailoverMultipleNodesReadWhileShuttingDown() throws Exception {
        final IgfsImpl igfs0 = nodeDatas[0].igfsImpl;

        clear(igfs0);

        IgfsAbstractSelfTest.create(igfs0, paths(DIR, SUBDIR), null);

        final int files = 500;

        final int fileSize = 16 * 1024;

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

        GridWorker gw = new GridWorker("grid-name", "shutdown-worker", log(), null) {
            @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
                Thread.sleep(5_000);

                // Now stop all the nodes but the 1st:
                for (int n = 1; n < numIgfsNodes; n++) {
                    stopGrid(n);

                    X.println("grid " + n + " stopped.");
                }
            }
        };
        new Thread(gw).start();

        // Read files repeatedly, while the nodes are being stopped:
        int readCnt = 0;

        while (readCnt < 40) {
            X.println("read " + readCnt);

            // Check files while the nodes are being stopped:
            for (int f = 0; f < files; f++) {
                IgfsPath path = filePath(f);

                byte[] data = createChunk(fileSize, f);

                // Check through 1st node:
                checkExist(igfs0, path);
                checkFileContent(igfs0, path, data);
            }

            readCnt++;
        }

        gw.join();
    }

    // files = 30, size = 2 * 1024 -- passes.
    // files = 30, size = 4 * 1024 -- corrupted file?.

    /** */
    private final int fileSize = 16 * 1024; // 16 * 1024

    /** */
    private final int files = 100; //500;

    /**
     *
     * @throws Exception
     */
    public void testFailoverMultipleNodesWriteReadWhileShuttingDown() throws Exception {
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

            int att = doWithRetries(2, new Callable<Void>() {
                @Override public Void call() throws Exception {
                    IgfsOutputStream ios = os;

                    try {
                        writeChunks0(igfs0, ios, f);
                    }
                    catch (IOException ioe) {
//                        try {
                            //ios = igfs0.create(filePath(f), 256, true, null, 0, -1, null);
                            ios = igfs0.append(filePath(f), false);

                            assert ios != null;

                            writeChunks0(igfs0, ios, f);
//                        }
//                        finally {
//                            ios.flush();
//                        }
                    }

                    return null;
                }
            });

            X.println("write #2 completed: " + f0 + " in " + att + " attempts.");
        }

        // Check files:
        for (int f = 0; f < files; f++) {
            IgfsPath path = filePath(f);

            byte[] data = createChunk(fileSize, f);

            // Check through 1st node:
            checkExist(igfs0, path);

            checkFileContent(igfs0, path, data, data);

            X.println("Read test completed: " + f);
        }
    }

    void writeChunks0(IgfsEx igfs0, IgfsOutputStream ios, int fileIndex) throws IOException {
        try {
            byte[] data = createChunk(fileSize, fileIndex);

            writeFileChunks(ios, data);
        }
        finally {
            ios.flush();

            U.closeQuiet(ios);

            //X.println("waiting for file to close: " + filePath(f));

            awaitFileClose(igfs0.asSecondary(), filePath(fileIndex));
        }
    }

//    protected void checkFileContentWithRetries(IgfsImpl igfs, IgfsPath file, int retries, @Nullable byte[]... chunks)
//        throws IOException , IgniteCheckedException {
//        int failureCnt = 0;
//
//        while (true) {
//            try {
//                checkFileContent(igfs, file, chunks);
//
//                break;
//            }
//            catch (IOException | IgniteCheckedException ie) {
//                failureCnt++;
//
//                if (failureCnt >= retries)
//                    throw ie;
//                else
//                    log().info("Failed to check file [" + file + "] content. Failure count = " + failureCnt);
//            }
//        }
//    }

//    /**
//     * Create the file in the given IGFS and write provided data chunks to it.
//     *
//     * @param igfs IGFS.
//     * @param file File.
//     * @param chunks Data chunks.
//     * @throws Exception If failed.
//     */
//    protected static void rewriteFile(IgfsImpl igfs, IgfsPath file,
//        @Nullable byte[]... chunks) throws Exception {
//        IgfsOutputStream os = null;
//
//        try {
//            os = igfs.append(file, true); //open(file, 256);
//
//            writeFileChunks(os, chunks);
//
//            assert igfs.size(file) == chunks[0].length;
//        }
//        finally {
//            U.closeQuiet(os);
//
//            awaitFileClose(igfs.asSecondary(), file);
//        }
//    }

    /**
     *
     * @param attempts
     * @param clo
     * @throws Exception
     */
    protected int doWithRetries(int attempts, Callable<Void> clo) throws Exception {
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