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
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.igfs.IgfsInputStream;
import org.apache.ignite.igfs.IgfsIpcEndpointConfiguration;
import org.apache.ignite.igfs.IgfsIpcEndpointType;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.IgfsOutputStream;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.igfs.IgfsMode.DUAL_ASYNC;
import static org.apache.ignite.igfs.IgfsMode.DUAL_SYNC;
import static org.apache.ignite.igfs.IgfsMode.PRIMARY;

/**
 * Test fo regular igfs operations.
 */
@SuppressWarnings({"ThrowableResultOfMethodCallIgnored", "ConstantConditions"})
public abstract class IgfsAbstractBaseSelfTest extends IgfsCommonAbstractTest {
    /** IGFS block size. */
    protected static final int IGFS_BLOCK_SIZE = 512 * 1024;

    /** Default block size (32Mb). */
    protected static final long BLOCK_SIZE = 32 * 1024 * 1024;

    /** Default repeat count. */
    protected static final int REPEAT_CNT = 5; // Diagnostic: up to 500; Regression: 5

    /** Concurrent operations count. */
    protected static final int OPS_CNT = 16;

    /** Renames count. */
    protected static final int RENAME_CNT = OPS_CNT;

    /** Deletes count. */
    protected static final int DELETE_CNT = OPS_CNT;

    /** Updates count. */
    protected static final int UPDATE_CNT = OPS_CNT;

    /** Mkdirs count. */
    protected static final int MKDIRS_CNT = OPS_CNT;

    /** Create count. */
    protected static final int CREATE_CNT = OPS_CNT;

    /** Time to wait until the caches get empty after format. */
    private static final long CACHE_EMPTY_TIMEOUT = 30_000L;

    /** Seed to generate random numbers. */
    protected static final long SEED = System.currentTimeMillis();

    /** Amount of blocks to prefetch. */
    protected static final int PREFETCH_BLOCKS = 1;

    /** Amount of sequential block reads before prefetch is triggered. */
    protected static final int SEQ_READS_BEFORE_PREFETCH = 2;

    /** Primary file system REST endpoint configuration map. */
    protected static final IgfsIpcEndpointConfiguration PRIMARY_REST_CFG;

    /** Secondary file system REST endpoint configuration map. */
    protected static final IgfsIpcEndpointConfiguration SECONDARY_REST_CFG;

    /** Directory. */
    protected static final IgfsPath DIR = new IgfsPath("/dir");

    /** Sub-directory. */
    protected static final IgfsPath SUBDIR = new IgfsPath(DIR, "subdir");

    /** Another sub-directory in the same directory. */
    protected static final IgfsPath SUBDIR2 = new IgfsPath(DIR, "subdir2");

    /** Sub-directory of the sub-directory. */
    protected static final IgfsPath SUBSUBDIR = new IgfsPath(SUBDIR, "subsubdir");

    /** File. */
    protected static final IgfsPath FILE = new IgfsPath(SUBDIR, "file");

    /** Another file in the same directory. */
    protected static final IgfsPath FILE2 = new IgfsPath(SUBDIR, "file2");

    /** Other directory. */
    protected static final IgfsPath DIR_NEW = new IgfsPath("/dirNew");

    /** Other subdirectory. */
    protected static final IgfsPath SUBDIR_NEW = new IgfsPath(DIR_NEW, "subdirNew");

    /** Other sub-directory of the sub-directory. */
    protected static final IgfsPath SUBSUBDIR_NEW = new IgfsPath(SUBDIR_NEW, "subsubdirNew");

    /** Other file. */
    protected static final IgfsPath FILE_NEW = new IgfsPath(SUBDIR_NEW, "fileNew");

    /** Default data chunk (128 bytes). */
    protected static final byte[] chunk = createChunk(128);

    /** Primary IGFS. */
    protected static IgfsImpl igfs;

    /** Secondary IGFS */
    protected static IgfsSecondaryFileSystem igfsSecondaryFileSystem;

    /** Secondary file system lower layer "backdoor" wrapped in UniversalFileSystemAdapter: */
    protected static IgfsSecondaryFileSystemTestAdapter igfsSecondary;

    /** IGFS mode. */
    protected final IgfsMode mode;

    /** Dual mode flag. */
    protected final boolean dual;

    /** IP finder for primary topology. */
    protected final TcpDiscoveryVmIpFinder primaryIpFinder = new TcpDiscoveryVmIpFinder(true);

    /** IP finder for secondary topology. */
    protected final TcpDiscoveryVmIpFinder secondaryIpFinder = new TcpDiscoveryVmIpFinder(true);

    /** Ignite nodes of cluster, excluding the secondary file system node, if any. */
    protected Ignite[] nodes;

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
    protected IgfsAbstractBaseSelfTest(IgfsMode mode) {
        this.mode = mode;

        dual = (mode == DUAL_SYNC || mode == DUAL_ASYNC);
    }

    /**
     * @return Relaxed consistency flag.
     */
    protected boolean relaxedConsistency() {
        return false;
    }

    /**
     * @return FragmentizerEnabled IGFS config flag.
     */
    protected boolean fragmentizerEnabled() {
        return true;
    }

    /**
     * @return Client flag.
     */
    protected boolean client() {
        return false;
    }

    /**
     * @return Whether append is supported.
     */
    protected boolean appendSupported() {
        return true;
    }

    /**
     * @return Whether permissions are supported.
     */
    protected boolean permissionsSupported() {
        return true;
    }

    /**
     * @return Whether properties are supported.
     */
    protected boolean propertiesSupported() {
        return true;
    }

    /**
     * @return Whether times are supported.
     */
    protected boolean timesSupported() {
        return true;
    }

    /**
     * @return Amount of nodes to start.
     */
    protected int nodeCount() {
        return 1;
    }

    /**
     * Data chunk.
     *
     * @param len Length.
     * @return Data chunk.
     */
    static byte[] createChunk(int len) {
        byte[] chunk = new byte[len];

        for (int i = 0; i < chunk.length; i++)
            chunk[i] = (byte)i;

        return chunk;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        igfsSecondaryFileSystem = createSecondaryFileSystemStack();

        nodes = new Ignite[nodeCount()];

        for (int i = 0; i < nodes.length; i++) {
            String nodeName = i == 0 ? "ignite" : "ignite" + i;

            nodes[i] = startGridWithIgfs(nodeName, "igfs", mode, igfsSecondaryFileSystem, PRIMARY_REST_CFG,
                primaryIpFinder);
        }

        igfs = (IgfsImpl) nodes[0].fileSystem("igfs");

        if (client()) {
            // Start client.
            Ignition.setClientMode(true);

            try {
                Ignite ignite = startGridWithIgfs("ignite-client", "igfs", mode, igfsSecondaryFileSystem,
                    PRIMARY_REST_CFG, primaryIpFinder);

                igfs = (IgfsImpl) ignite.fileSystem("igfs");
            }
            finally {
                Ignition.setClientMode(false);
            }
        }
    }

    /**
     * Creates secondary file system stack.
     *
     * @return The secondary file system.
     * @throws Exception On error.
     */
    protected IgfsSecondaryFileSystem createSecondaryFileSystemStack() throws Exception {
        Ignite igniteSecondary = startGridWithIgfs("ignite-secondary", "igfs-secondary", PRIMARY, null,
            SECONDARY_REST_CFG, secondaryIpFinder);

        IgfsEx secondaryIgfsImpl = (IgfsEx) igniteSecondary.fileSystem("igfs-secondary");

        igfsSecondary = new DefaultIgfsSecondaryFileSystemTestAdapter(secondaryIgfsImpl);

        return secondaryIgfsImpl.asSecondary();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        clear(igfs, igfsSecondary);

        assert igfs.listFiles(IgfsPath.ROOT).isEmpty();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        G.stopAll(true);
    }

    /**
     * Start grid with IGFS.
     *
     * @param igniteInstanceName Ignite instance name.
     * @param igfsName IGFS name
     * @param mode IGFS mode.
     * @param secondaryFs Secondary file system (optional).
     * @param restCfg Rest configuration string (optional).
     * @param ipFinder IP finder.
     * @return Started grid instance.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    protected Ignite startGridWithIgfs(String igniteInstanceName, String igfsName, IgfsMode mode,
        @Nullable IgfsSecondaryFileSystem secondaryFs, @Nullable IgfsIpcEndpointConfiguration restCfg,
        TcpDiscoveryIpFinder ipFinder) throws Exception {
        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setName(igfsName);
        igfsCfg.setBlockSize(IGFS_BLOCK_SIZE);
        igfsCfg.setDefaultMode(mode);
        igfsCfg.setIpcEndpointConfiguration(restCfg);
        igfsCfg.setSecondaryFileSystem(secondaryFs);
        igfsCfg.setPrefetchBlocks(PREFETCH_BLOCKS);
        igfsCfg.setSequentialReadsBeforePrefetch(SEQ_READS_BEFORE_PREFETCH);
        igfsCfg.setRelaxedConsistency(relaxedConsistency());
        igfsCfg.setFragmentizerEnabled(fragmentizerEnabled());

        CacheConfiguration dataCacheCfg = defaultCacheConfiguration();

        dataCacheCfg.setNearConfiguration(null);
        dataCacheCfg.setCacheMode(PARTITIONED);
        dataCacheCfg.setNearConfiguration(null);
        dataCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        dataCacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(2));
        dataCacheCfg.setBackups(0);
        dataCacheCfg.setAtomicityMode(TRANSACTIONAL);

        CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setNearConfiguration(null);
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        prepareCacheConfigurations(dataCacheCfg, metaCacheCfg);

        igfsCfg.setDataCacheConfiguration(dataCacheCfg);
        igfsCfg.setMetaCacheConfiguration(metaCacheCfg);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setIgniteInstanceName(igniteInstanceName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);
        cfg.setFileSystemConfiguration(igfsCfg);

        cfg.setLocalHost("127.0.0.1");
        cfg.setConnectorConfiguration(null);

        return G.start(cfg);
    }

    /**
     * Prepare cache configuration.
     *
     * @param dataCacheCfg Data cache configuration.
     * @param metaCacheCfg Meta cache configuration.
     */
    protected void prepareCacheConfigurations(CacheConfiguration dataCacheCfg, CacheConfiguration metaCacheCfg) {
        // Noop
    }

    /**
     * Execute provided task in a separate thread.
     *
     * @param task Task to execute.
     * @return Result.
     */
    protected static <T> IgniteInternalFuture<T> execute(final Callable<T> task) {
        final GridFutureAdapter<T> fut = new GridFutureAdapter<>();

        new Thread(new Runnable() {
            @Override public void run() {
                try {
                    fut.onDone(task.call());
                }
                catch (Throwable e) {
                    fut.onDone(e);
                }
            }
        }).start();

        return fut;
    }


    /**
     * Create the given directories and files in the given IGFS.
     *
     * @param igfs IGFS.
     * @param dirs Directories.
     * @param files Files.
     * @throws Exception If failed.
     */
    @SuppressWarnings("EmptyTryBlock")
    public static void create(IgfsImpl igfs, @Nullable IgfsPath[] dirs, @Nullable IgfsPath[] files) throws Exception {
        if (dirs != null) {
            for (IgfsPath dir : dirs)
                igfs.mkdirs(dir);
        }

        if (files != null) {
            for (IgfsPath file : files) {
                try (OutputStream ignored = igfs.create(file, true)) {
                    // No-op.
                }

                igfs.await(file);
            }
        }
    }

    /**
     * Creates specified files/directories
     *
     * @param uni The file system to operate on.
     * @param dirs The directories to create.
     * @param files The files to create.
     * @throws Exception On error.
     */
    @SuppressWarnings("EmptyTryBlock")
    public void create(IgfsSecondaryFileSystemTestAdapter uni, @Nullable IgfsPath[] dirs, @Nullable IgfsPath[] files)
        throws Exception {
        if (dirs != null) {
            for (IgfsPath dir : dirs)
                uni.mkdirs(dir.toString());
        }

        if (files != null) {
            for (IgfsPath file : files)
                try (OutputStream ignore = uni.openOutputStream(file.toString(), false)) {
                    // No-op
                }
        }
    }

    /**
     * Create the file in the given IGFS and write provided data chunks to it.
     *
     * @param igfs IGFS.
     * @param file File.
     * @param overwrite Overwrite flag.
     * @param chunks Data chunks.
     * @throws IOException In case of IO exception.
     */
    protected static void createFile(IgfsEx igfs, IgfsPath file, boolean overwrite, @Nullable byte[]... chunks)
        throws IOException {
        OutputStream os = null;

        try {
            os = igfs.create(file, overwrite);

            writeFileChunks(os, chunks);
        }
        finally {
            U.closeQuiet(os);

            awaitFileClose(igfs, file);
        }
    }

    /**
     * Create the file in the given IGFS and write provided data chunks to it.
     *
     * @param uni FS tests adaptor.
     * @param file File.
     * @param chunks Data chunks.
     * @throws IOException In case of IO exception.
     */
    protected static void createFile(IgfsSecondaryFileSystemTestAdapter uni, IgfsPath file, @Nullable byte[]... chunks)
        throws IOException {
        OutputStream os = null;

        try {
            os = uni.openOutputStream(file.toString(), false);

            writeFileChunks(os, chunks);
        }
        finally {
            U.closeQuiet(os);

            IgfsEx igfsEx = uni.igfs();

            if (igfsEx != null)
                awaitFileClose(igfsEx, file);
        }
    }

    /**
     * Create the file in the given IGFS and write provided data chunks to it.
     *
     * @param igfs IGFS.
     * @param file File.
     * @param overwrite Overwrite flag.
     * @param blockSize Block size.
     * @param chunks Data chunks.
     * @throws Exception If failed.
     */
    protected static void createFile(IgfsImpl igfs, IgfsPath file, boolean overwrite, long blockSize,
        @Nullable byte[]... chunks) throws Exception {
        IgfsOutputStream os = null;

        try {
            os = igfs.create(file, 256, overwrite, null, 0, blockSize, null);

            writeFileChunks(os, chunks);
        }
        finally {
            U.closeQuiet(os);

            awaitFileClose(igfs, file);
        }
    }

    /**
     * Append to the file in the given IGFS provided data chunks.
     *
     * @param igfs IGFS.
     * @param file File.
     * @param chunks Data chunks.
     * @throws Exception If failed.
     */
    protected static void appendFile(IgfsImpl igfs, IgfsPath file, @Nullable byte[]... chunks)
        throws Exception {
        IgfsOutputStream os = null;

        try {
            os = igfs.append(file, false);

            writeFileChunks(os, chunks);
        }
        finally {
            U.closeQuiet(os);

            awaitFileClose(igfs, file);
        }
    }

    /**
     * Write provided data chunks to the file output stream.
     *
     * @param os Output stream.
     * @param chunks Data chunks.
     * @throws IOException If failed.
     */
    protected static void writeFileChunks(OutputStream os, @Nullable byte[]... chunks) throws IOException {
        if (chunks != null && chunks.length > 0) {
            for (byte[] chunk : chunks)
                os.write(chunk);
        }
    }

    /**
     * Await for previously opened output stream to close. This is achieved by requesting dummy update on the file.
     *
     * @param igfs IGFS.
     * @param file File.
     */
    public static void awaitFileClose(IgfsSecondaryFileSystem igfs, IgfsPath file) {
        try {
            igfs.update(file, Collections.singletonMap("prop", "val"));
        }
        catch (IgniteException ignore) {
            // No-op.
        }
    }

    /**
     * Await for previously opened output stream to close.
     *
     * @param igfs IGFS.
     * @param file File.
     */
    public static void awaitFileClose(@Nullable IgfsEx igfs, IgfsPath file) {
        igfs.await(file);
    }

    /**
     * Ensure that the given paths exist in the given IGFSs.
     *
     * @param igfs First IGFS.
     * @param igfsSecondary Second IGFS.
     * @param paths Paths.
     * @throws Exception If failed.
     */
    protected void checkExist(IgfsImpl igfs, IgfsSecondaryFileSystemTestAdapter igfsSecondary, IgfsPath... paths)
        throws Exception {
        checkExist(igfs, paths);

        if (dual)
            checkExist(igfsSecondary, paths);
    }

    /**
     * Ensure that the given paths exist in the given IGFS.
     *
     * @param igfs IGFS.
     * @param paths Paths.
     * @throws IgniteCheckedException If failed.
     */
    protected static void checkExist(IgfsImpl igfs, IgfsPath... paths) throws IgniteCheckedException {
        for (IgfsPath path : paths)
            assert igfs.exists(path) : "Path doesn't exist [igfs=" + igfs.name() + ", path=" + path + ']';
    }

    /**
     * Ensure that the given paths exist in the given IGFS.
     *
     * @param uni filesystem.
     * @param paths Paths.
     * @throws IgniteCheckedException If failed.
     */
    protected void checkExist(IgfsSecondaryFileSystemTestAdapter uni, IgfsPath... paths) throws IgniteCheckedException {
        IgfsEx ex = uni.igfs();

        for (IgfsPath path : paths) {
            if (ex != null)
                assert ex.context().meta().fileId(path) != null : "Path doesn't exist [igfs=" + ex.name() +
                    ", path=" + path + ']';

            try {
                assert uni.exists(path.toString()) : "Path doesn't exist [igfs=" + uni.name() + ", path=" + path + ']';
            }
            catch (IOException ioe) {
                throw new IgniteCheckedException(ioe);
            }
        }
    }

    /**
     * Ensure that the given paths don't exist in the given IGFSs.
     *
     * @param igfs First IGFS.
     * @param igfsSecondary Second IGFS.
     * @param paths Paths.
     * @throws Exception If failed.
     */
    protected void checkNotExist(IgfsImpl igfs, IgfsSecondaryFileSystemTestAdapter igfsSecondary, IgfsPath... paths)
        throws Exception {
        checkNotExist(igfs, paths);

        if (mode != PRIMARY)
            checkNotExist(igfsSecondary, paths);
    }

    /**
     * Ensure that the given paths don't exist in the given IGFS.
     *
     * @param igfs IGFS.
     * @param paths Paths.
     * @throws Exception If failed.
     */
    protected void checkNotExist(IgfsImpl igfs, IgfsPath... paths) throws Exception {
        for (IgfsPath path : paths)
            assert !igfs.exists(path) : "Path exists [igfs=" + igfs.name() + ", path=" + path + ']';
    }

    /**
     * Ensure that the given paths don't exist in the given IGFS.
     *
     * @param uni secondary FS.
     * @param paths Paths.
     * @throws Exception If failed.
     */
    protected void checkNotExist(IgfsSecondaryFileSystemTestAdapter uni, IgfsPath... paths) throws Exception {
        IgfsEx ex = uni.igfs();

        for (IgfsPath path : paths) {
            if (ex != null)
                assert !ex.exists(path) : "Path exists [igfs=" + ex.name() + ", path=" + path + ']';

            assert !uni.exists(path.toString()) : "Path exists [igfs=" + uni.name() + ", path=" + path + ']';
        }
    }

    /**
     * Ensure that the given file exists in the given IGFSs and that it has exactly the same content as provided in the
     * "data" parameter.
     *
     * @param igfs First IGFS.
     * @param igfsSecondary Second IGFS.
     * @param file File.
     * @param chunks Expected data.
     * @throws Exception If failed.
     */
    protected void checkFile(@Nullable IgfsImpl igfs, IgfsSecondaryFileSystemTestAdapter igfsSecondary, IgfsPath file,
        @Nullable byte[]... chunks) throws Exception {
        if (igfs != null) {
            checkExist(igfs, file);
            checkFileContent(igfs, file, chunks);
        }

        if (dual) {
            checkExist(igfsSecondary, file);
            checkFileContent(igfsSecondary, file.toString(), chunks);
        }
    }

    /**
     * Ensure that the given file has exactly the same content as provided in the "data" parameter.
     *
     * @param igfs IGFS.
     * @param file File.
     * @param chunks Expected data.
     * @throws IOException In case of IO exception.
     * @throws IgniteCheckedException In case of Grid exception.
     */
    protected static void checkFileContent(IgfsImpl igfs, IgfsPath file, @Nullable byte[]... chunks)
        throws IOException, IgniteCheckedException {
        if (chunks != null && chunks.length > 0) {
            IgfsInputStream is = null;

            try {
                is = igfs.open(file);

                int chunkIdx = 0;
                int pos = 0;

                for (byte[] chunk : chunks) {
                    byte[] buf = new byte[chunk.length];

                    is.readFully(pos, buf);

                    assert Arrays.equals(chunk, buf) : "Bad chunk [igfs=" + igfs.name() + ", chunkIdx=" + chunkIdx +
                        ", expected=" + Arrays.toString(chunk) + ", actual=" + Arrays.toString(buf) + ']';

                    chunkIdx++;
                    pos += chunk.length;
                }

                is.close();
            }
            finally {
                U.closeQuiet(is);
            }
        }
    }

    /**
     * Ensure that the given file has exactly the same content as provided in the "data" parameter.
     *
     * @param uni FS.
     * @param path File.
     * @param chunks Expected data.
     * @throws IOException In case of IO exception.
     * @throws IgniteCheckedException In case of Grid exception.
     */
    protected void checkFileContent(IgfsSecondaryFileSystemTestAdapter uni, String path, @Nullable byte[]... chunks)
        throws IOException, IgniteCheckedException {
        if (chunks != null && chunks.length > 0) {
            InputStream is = null;

            try {
                is = uni.openInputStream(path);

                int chunkIdx = 0;

                int read;
                for (byte[] chunk: chunks) {
                    byte[] buf = new byte[chunk.length];

                    read = 0;

                    while (true) {
                        int r = is.read(buf, read, buf.length - read);

                        read += r;

                        if (read == buf.length || r <= 0)
                            break;
                    }

                    assert read == chunk.length : "Chunk #" + chunkIdx + " was not read fully:" +
                            " read=" + read + ", expected=" + chunk.length;
                    assert Arrays.equals(chunk, buf) : "Bad chunk [igfs=" + uni.name() + ", chunkIdx=" + chunkIdx +
                        ", expected=" + Arrays.toString(chunk) + ", actual=" + Arrays.toString(buf) + ']';

                    chunkIdx++;
                }

                is.close();
            }
            finally {
                U.closeQuiet(is);
            }
        }
    }

    /**
     * Create map with properties.
     *
     * @param grpName Group name.
     * @param perm Permission.
     * @return Map with properties.
     */
    protected Map<String, String> properties(@Nullable String grpName, @Nullable String perm) {
        Map<String, String> props = new HashMap<>();

        if (grpName != null)
            props.put(IgfsUtils.PROP_GROUP_NAME, grpName);

        if (perm != null)
            props.put(IgfsUtils.PROP_PERMISSION, perm);

        return props;
    }

    /**
     * Create map with properties.
     *
     * @param username User name.
     * @param grpName Group name.
     * @param perm Permission.
     * @return Map with properties.
     */
    protected Map<String, String> properties(@Nullable String username, @Nullable String grpName,
        @Nullable String perm) {
        Map<String, String> props = new HashMap<>();

        if (username != null)
            props.put(IgfsUtils.PROP_USER_NAME, username);

        if (grpName != null)
            props.put(IgfsUtils.PROP_GROUP_NAME, grpName);

        if (perm != null)
            props.put(IgfsUtils.PROP_PERMISSION, perm);

        return props;
    }

    /**
     * Convenient method to group paths.
     *
     * @param paths Paths to group.
     * @return Paths as array.
     */
    protected static IgfsPath[] paths(IgfsPath... paths) {
        return paths;
    }

    /**
     * Safely clear IGFSs.
     *
     * @param igfs First IGFS.
     * @param igfsSecondary Second IGFS.
     * @throws Exception If failed.
     */
    protected void clear(IgniteFileSystem igfs, IgfsSecondaryFileSystemTestAdapter igfsSecondary) throws Exception {
        clear(igfs);

        if (mode != PRIMARY)
            clear(igfsSecondary);
    }

    /**
     * Gets the data cache instance for this IGFS instance.
     *
     * @param igfs The IGFS unstance.
     * @return The data cache.
     */
    protected static GridCacheAdapter<IgfsBlockKey, byte[]> getDataCache(IgniteFileSystem igfs) {
        String dataCacheName = igfs.configuration().getDataCacheConfiguration().getName();

        IgniteEx igniteEx = ((IgfsEx)igfs).context().kernalContext().grid();

        return ((IgniteKernal)igniteEx).internalCache(dataCacheName);
    }

    /**
     * Gets meta cache.
     *
     * @param igfs The IGFS instance.
     * @return The data cache.
     */
    protected static GridCacheAdapter<IgniteUuid, IgfsEntryInfo> getMetaCache(IgniteFileSystem igfs) {
        String dataCacheName = igfs.configuration().getMetaCacheConfiguration().getName();

        IgniteEx igniteEx = ((IgfsEx)igfs).context().kernalContext().grid();

        return ((IgniteKernal)igniteEx).internalCache(dataCacheName);
    }

    /**
     * Clear particular IGFS.
     *
     * @param igfs IGFS.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public static void clear(IgniteFileSystem igfs) throws Exception {
        Field workerMapFld = IgfsImpl.class.getDeclaredField("workerMap");

        workerMapFld.setAccessible(true);

        // Wait for all workers to finish.
        Map<IgfsPath, IgfsFileWorkerBatch> workerMap = (Map<IgfsPath, IgfsFileWorkerBatch>)workerMapFld.get(igfs);

        for (Map.Entry<IgfsPath, IgfsFileWorkerBatch> entry : workerMap.entrySet()) {
            entry.getValue().cancel();

            try {
                entry.getValue().await();
            }
            catch (IgniteCheckedException e) {
                if (!(e instanceof IgfsFileWorkerBatchCancelledException))
                    throw e;
            }
        }

        // Clear igfs.
        igfs.clear();

        int prevDifferentSize = Integer.MAX_VALUE; // Previous different size.
        int constCnt = 0, totalCnt = 0;
        final int constThreshold = 20;
        final long sleepPeriod = 500L;
        final long totalThreshold = CACHE_EMPTY_TIMEOUT / sleepPeriod;

        while (true) {
            int metaSize = 0;

            for (IgniteUuid metaId : getMetaCache(igfs).keySet()) {
                if (!IgfsUtils.isRootOrTrashId(metaId))
                    metaSize++;
            }

            int dataSize = getDataCache(igfs).size();

            int size = metaSize + dataSize;

            if (size <= 2)
                return; // Caches are cleared, we're done. (2 because ROOT & TRASH always exist).

            X.println("Sum size: " + size);

            if (size > prevDifferentSize) {
                X.println("Summary cache size has grown unexpectedly: size=" + size + ", prevSize=" + prevDifferentSize);

                break;
            }

            if (totalCnt > totalThreshold) {
                X.println("Timeout exceeded.");

                break;
            }

            if (size == prevDifferentSize) {
                constCnt++;

                if (constCnt == constThreshold) {
                    X.println("Summary cache size stays unchanged for too long: size=" + size);

                    break;
                }
            } else {
                constCnt = 0;

                prevDifferentSize = size; // renew;
            }

            Thread.sleep(sleepPeriod);

            totalCnt++;
        }

        dumpCache("MetaCache" , getMetaCache(igfs));

        dumpCache("DataCache" , getDataCache(igfs));

        fail("Caches are not empty.");
    }

    /**
     * Dumps given cache for diagnostic purposes.
     *
     * @param cacheName Name.
     * @param cache The cache.
     */
    private static void dumpCache(String cacheName, GridCacheAdapter<?,?> cache) {
        X.println("=============================== " + cacheName + " cache dump: ");

        Iterable<? extends GridCacheEntryEx> entries = cache.entries();

        for (GridCacheEntryEx e: entries)
            X.println("Lost " + cacheName + " entry = " + e);
    }

    /**
     * Clear particular {@link IgfsSecondaryFileSystemTestAdapter}.
     *
     * @param uni IGFS.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public static void clear(IgfsSecondaryFileSystemTestAdapter uni) throws Exception {
        IgfsEx igfsEx = uni.igfs();

        if (igfsEx != null)
            clear(igfsEx);

        // Clear the filesystem.
        uni.format();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        clear(igfs, igfsSecondary);
    }
}