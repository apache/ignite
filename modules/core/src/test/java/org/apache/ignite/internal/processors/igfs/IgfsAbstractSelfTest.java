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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.igfs.IgfsDirectoryNotEmptyException;
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.igfs.IgfsInputStream;
import org.apache.ignite.igfs.IgfsIpcEndpointConfiguration;
import org.apache.ignite.igfs.IgfsIpcEndpointType;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.IgfsOutputStream;
import org.apache.ignite.igfs.IgfsParentNotDirectoryException;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.IgfsPathNotFoundException;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystem;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;

import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMemoryMode.ONHEAP_TIERED;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.igfs.IgfsMode.PRIMARY;
import static org.apache.ignite.igfs.IgfsMode.PROXY;

/**
 * Test fo regular igfs operations.
 */
@SuppressWarnings({"ThrowableResultOfMethodCallIgnored", "ConstantConditions"})
public abstract class IgfsAbstractSelfTest extends IgfsCommonAbstractTest {
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

    /** Memory mode. */
    protected final CacheMemoryMode memoryMode;

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
    protected IgfsAbstractSelfTest(IgfsMode mode) {
        this(mode, ONHEAP_TIERED);
    }

    /**
     * Constructor.
     *
     * @param mode IGFS mode.
     * @param memoryMode Memory mode.
     */
    protected IgfsAbstractSelfTest(IgfsMode mode, CacheMemoryMode memoryMode) {
        assert mode != null && mode != PROXY;

        this.mode = mode;
        this.memoryMode = memoryMode;

        dual = mode != PRIMARY;
    }

    /**
     * @return Relaxed consistency flag.
     */
    protected boolean relaxedConsistency() {
        return false;
    }

    /**
     * @return Relaxed consistency flag.
     */
    protected boolean initializeDefaultPathModes() {
        return false;
    }

    /**
     * @return Client flag.
     */
    protected boolean client() {
        return false;
    }

    /**
     * @return Use optimized marshaller flag.
     */
    protected boolean useOptimizedMarshaller() {
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

        assert igfs.listFiles(new IgfsPath("/")).isEmpty();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        G.stopAll(true);
    }

    /**
     * Start grid with IGFS.
     *
     * @param gridName Grid name.
     * @param igfsName IGFS name
     * @param mode IGFS mode.
     * @param secondaryFs Secondary file system (optional).
     * @param restCfg Rest configuration string (optional).
     * @param ipFinder IP finder.
     * @return Started grid instance.
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    protected Ignite startGridWithIgfs(String gridName, String igfsName, IgfsMode mode,
        @Nullable IgfsSecondaryFileSystem secondaryFs, @Nullable IgfsIpcEndpointConfiguration restCfg,
        TcpDiscoveryIpFinder ipFinder) throws Exception {
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
        igfsCfg.setRelaxedConsistency(relaxedConsistency());

        igfsCfg.setInitializeDefaultPathModes(initializeDefaultPathModes());

        CacheConfiguration dataCacheCfg = defaultCacheConfiguration();

        dataCacheCfg.setName("dataCache");
        dataCacheCfg.setNearConfiguration(null);
        dataCacheCfg.setCacheMode(PARTITIONED);
        dataCacheCfg.setNearConfiguration(null);
        dataCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        dataCacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(2));
        dataCacheCfg.setBackups(0);
        dataCacheCfg.setAtomicityMode(TRANSACTIONAL);
        dataCacheCfg.setMemoryMode(memoryMode);
        dataCacheCfg.setOffHeapMaxMemory(0);

        CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setName("metaCache");
        metaCacheCfg.setNearConfiguration(null);
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        IgniteConfiguration cfg = new IgniteConfiguration();

        if (useOptimizedMarshaller())
            cfg.setMarshaller(new OptimizedMarshaller());

        cfg.setGridName(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        prepareCacheConfigurations(dataCacheCfg, metaCacheCfg);

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(dataCacheCfg, metaCacheCfg);
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
     * Test existence check when the path exists both locally and remotely.
     *
     * @throws Exception If failed.
     */
    public void testExists() throws Exception {
        create(igfs.asSecondary(), paths(DIR), null);

        checkExist(igfs, igfsSecondary, DIR);
    }

    /**
     * Test existence check when the path doesn't exist remotely.
     *
     * @throws Exception If failed.
     */
    public void testExistsPathDoesNotExist() throws Exception {
        assert !igfs.exists(DIR);
    }

    /**
     * Test list files routine.
     *
     * @throws Exception If failed.
     */
    public void testListFiles() throws Exception {
        create(igfs, paths(DIR, SUBDIR, SUBSUBDIR), paths(FILE));

        Collection<IgfsFile> paths = igfs.listFiles(SUBDIR);

        assert paths != null;
        assert paths.size() == 2;

        Iterator<IgfsFile> iter = paths.iterator();

        IgfsFile path1 = iter.next();
        IgfsFile path2 = iter.next();

        assert (SUBSUBDIR.equals(path1.path()) && FILE.equals(path2.path())) ||
            (FILE.equals(path1.path()) && SUBSUBDIR.equals(path2.path()));
    }

    /**
     * Test list files routine when the path doesn't exist remotely.
     *
     * @throws Exception If failed.
     */
    public void testListFilesPathDoesNotExist() throws Exception {
        Collection<IgfsFile> paths = null;

        try {
            paths = igfs.listFiles(SUBDIR);
        }
        catch (IgniteException ignore) {
            // No-op.
        }

        assert paths == null || paths.isEmpty();
    }

    /**
     * Test info routine when the path exists both locally and remotely.
     *
     * @throws Exception If failed.
     */
    public void testInfo() throws Exception {
        create(igfs, paths(DIR), null);

        IgfsFile info = igfs.info(DIR);

        assert info != null;

        assertEquals(DIR, info.path());
    }

    /**
     * Test info routine when the path doesn't exist remotely.
     *
     * @throws Exception If failed.
     */
    public void testInfoPathDoesNotExist() throws Exception {
        IgfsFile info = null;

        try {
            info = igfs.info(DIR);
        }
        catch (IgniteException ignore) {
            // No-op.
        }

        assert info == null;
    }

    /**
     * Test rename in case both local and remote file systems have the same folder structure and the path being renamed
     * is a file.
     *
     * @throws Exception If failed.
     */
    public void testRenameFile() throws Exception {
        create(igfs, paths(DIR, SUBDIR), paths(FILE));

        igfs.rename(FILE, FILE2);

        checkExist(igfs, igfsSecondary, FILE2);
        checkNotExist(igfs, igfsSecondary, FILE);
    }

    /**
     * Test file rename when parent folder is the root.
     *
     * @throws Exception If failed.
     */
    public void testRenameFileParentRoot() throws Exception {
        IgfsPath file1 = new IgfsPath("/file1");
        IgfsPath file2 = new IgfsPath("/file2");

        create(igfs.asSecondary(), null, paths(file1));

        igfs.rename(file1, file2);

        checkExist(igfs, igfsSecondary, file2);
        checkNotExist(igfs, igfsSecondary, file1);
    }

    /**
     * Test rename in case both local and remote file systems have the same folder structure and the path being renamed
     * is a directory.
     *
     * @throws Exception If failed.
     */
    public void testRenameDirectory() throws Exception {
        create(igfs, paths(DIR, SUBDIR), null);

        igfs.rename(SUBDIR, SUBDIR2);

        checkExist(igfs, igfsSecondary, SUBDIR2);
        checkNotExist(igfs, igfsSecondary, SUBDIR);
    }

    /**
     * Test directory rename when parent folder is the root.
     *
     * @throws Exception If failed.
     */
    public void testRenameDirectoryParentRoot() throws Exception {
        IgfsPath dir1 = new IgfsPath("/dir1");
        IgfsPath dir2 = new IgfsPath("/dir2");

        create(igfs.asSecondary(), paths(dir1), null);

        igfs.rename(dir1, dir2);

        checkExist(igfs, igfsSecondary, dir2);
        checkNotExist(igfs, igfsSecondary, dir1);
    }

    /**
     * Test move in case both local and remote file systems have the same folder structure and the path being renamed is
     * a file.
     *
     * @throws Exception If failed.
     */
    public void testMoveFile() throws Exception {
        create(igfs, paths(DIR, SUBDIR, DIR_NEW, SUBDIR_NEW), paths(FILE));

        igfs.rename(FILE, SUBDIR_NEW);

        checkExist(igfs, igfsSecondary, new IgfsPath(SUBDIR_NEW, FILE.name()));
        checkNotExist(igfs, igfsSecondary, FILE);
    }

    /**
     * Test file move when destination is the root.
     *
     * @throws Exception If failed.
     */
    public void testMoveFileDestinationRoot() throws Exception {
        create(igfs.asSecondary(), paths(DIR, SUBDIR), paths(FILE));

        igfs.rename(FILE, new IgfsPath());

        checkExist(igfs, igfsSecondary, new IgfsPath("/" + FILE.name()));
        checkNotExist(igfs, igfsSecondary, FILE);
    }

    /**
     * Test file move when source parent is the root.
     *
     * @throws Exception If failed.
     */
    public void testMoveFileSourceParentRoot() throws Exception {
        IgfsPath file = new IgfsPath("/" + FILE.name());

        create(igfs, paths(DIR_NEW, SUBDIR_NEW), paths(file));

        igfs.rename(file, SUBDIR_NEW);

        checkExist(igfs, igfsSecondary, new IgfsPath(SUBDIR_NEW, FILE.name()));
        checkNotExist(igfs, igfsSecondary, file);
    }

    /**
     * Test move and rename in case both local and remote file systems have the same folder structure and the path being
     * renamed is a file.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameFile() throws Exception {
        create(igfs, paths(DIR, SUBDIR, DIR_NEW, SUBDIR_NEW), paths(FILE));

        igfs.rename(FILE, FILE_NEW);

        checkExist(igfs, igfsSecondary, FILE_NEW);
        checkNotExist(igfs, igfsSecondary, FILE);
    }

    /**
     * Test file move and rename when destination is the root.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameFileDestinationRoot() throws Exception {
        IgfsPath file = new IgfsPath("/" + FILE.name());

        create(igfs, paths(DIR, SUBDIR), paths(FILE));

        igfs.rename(FILE, file);

        checkExist(igfs, igfsSecondary, file);
        checkNotExist(igfs, igfsSecondary, FILE);
    }

    /**
     * Test file move and rename when source parent is the root.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameFileSourceParentRoot() throws Exception {
        IgfsPath file = new IgfsPath("/" + FILE_NEW.name());

        create(igfs, paths(DIR_NEW, SUBDIR_NEW), paths(file));

        igfs.rename(file, FILE_NEW);

        checkExist(igfs, igfsSecondary, FILE_NEW);
        checkNotExist(igfs, igfsSecondary, file);
    }

    /**
     * Test move in case both local and remote file systems have the same folder structure and the path being renamed is
     * a directory.
     *
     * @throws Exception If failed.
     */
    public void testMoveDirectory() throws Exception {
        create(igfs, paths(DIR, SUBDIR, SUBSUBDIR, DIR_NEW, SUBDIR_NEW), null);

        igfs.rename(SUBSUBDIR, SUBDIR_NEW);

        checkExist(igfs, igfsSecondary, new IgfsPath(SUBDIR_NEW, SUBSUBDIR.name()));
        checkNotExist(igfs, igfsSecondary, SUBSUBDIR);
    }

    /**
     * Test directory move when destination is the root.
     *
     * @throws Exception If failed.
     */
    public void testMoveDirectoryDestinationRoot() throws Exception {
        create(igfs.asSecondary(), paths(DIR, SUBDIR, SUBSUBDIR), null);

        igfs.rename(SUBSUBDIR, new IgfsPath());

        checkExist(igfs, igfsSecondary, new IgfsPath("/" + SUBSUBDIR.name()));
        checkNotExist(igfs, igfsSecondary, SUBSUBDIR);
    }

    /**
     * Test directory move when source parent is the root.
     *
     * @throws Exception If failed.
     */
    public void testMoveDirectorySourceParentRoot() throws Exception {
        IgfsPath dir = new IgfsPath("/" + SUBSUBDIR.name());

        create(igfs.asSecondary(), paths(DIR_NEW, SUBDIR_NEW, dir), null);

        igfs.rename(dir, SUBDIR_NEW);

        checkExist(igfs, igfsSecondary, new IgfsPath(SUBDIR_NEW, SUBSUBDIR.name()));
        checkNotExist(igfs, igfsSecondary, dir);
    }

    /**
     * Test move and rename  in case both local and remote file systems have the same folder structure and the path
     * being renamed is a directory.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameDirectory() throws Exception {
        create(igfs, paths(DIR, SUBDIR, SUBSUBDIR, DIR_NEW, SUBDIR_NEW), null);

        igfs.rename(SUBSUBDIR, SUBSUBDIR_NEW);

        checkExist(igfs, igfsSecondary, SUBSUBDIR_NEW);
        checkNotExist(igfs, igfsSecondary, SUBSUBDIR);
    }

    /**
     * Test directory move and rename when destination is the root.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameDirectoryDestinationRoot() throws Exception {
        IgfsPath dir = new IgfsPath("/" + SUBSUBDIR.name());

        create(igfs, paths(DIR, SUBDIR, SUBSUBDIR), null);

        igfs.rename(SUBSUBDIR, dir);

        checkExist(igfs, igfsSecondary, dir);
        checkNotExist(igfs, igfsSecondary, SUBSUBDIR);
    }

    /**
     * Test directory move and rename when source parent is the root.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameDirectorySourceParentRoot() throws Exception {
        IgfsPath dir = new IgfsPath("/" + SUBSUBDIR_NEW.name());

        create(igfs, paths(DIR_NEW, SUBDIR_NEW, dir), null);

        igfs.rename(dir, SUBSUBDIR_NEW);

        checkExist(igfs, igfsSecondary, SUBSUBDIR_NEW);
        checkNotExist(igfs, igfsSecondary, dir);
    }

    /**
     * Ensure that rename doesn't occur in case source doesn't exist remotely.
     *
     * @throws Exception If failed.
     */
    public void testMoveRenameSourceDoesNotExist() throws Exception {
        create(igfs, paths(DIR, DIR_NEW), null);

        GridTestUtils.assertThrowsInherited(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                igfs.rename(SUBDIR, SUBDIR_NEW);

                return null;
            }
        }, IgfsException.class, null);

        checkNotExist(igfs, igfsSecondary, SUBDIR, SUBDIR_NEW);
    }

    /**
     * Test mkdirs in case both local and remote file systems have the same folder structure.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    public void testMkdirs() throws Exception {
        if (!propertiesSupported())
            return;

        Map<String, String> props = properties(null, null, "0555"); // mkdirs command doesn't propagate user info.

        igfs.mkdirs(new IgfsPath("/x"), null);
        checkExist(igfs, igfsSecondary, new IgfsPath("/x"));

        igfs.mkdirs(new IgfsPath("/k/l"), null);
        checkExist(igfs, igfsSecondary, new IgfsPath("/k/l"));

        igfs.mkdirs(new IgfsPath("/x/y"), null);
        checkExist(igfs, igfsSecondary, new IgfsPath("/x/y"));

        igfs.mkdirs(new IgfsPath("/a/b/c/d"), null);
        checkExist(igfs, igfsSecondary, new IgfsPath("/a/b/c/d"));

        igfs.mkdirs(new IgfsPath("/a/b/c/d/e"), null);
        checkExist(igfs, igfsSecondary, new IgfsPath("/a/b/c/d/e"));

        create(igfs, null, new IgfsPath[] { new IgfsPath("/d/f") }); // "f" is a file.
        checkExist(igfs, igfsSecondary, new IgfsPath("/d/f"));
        assertTrue(igfs.info(new IgfsPath("/d/f")).isFile());

        try {
            igfs.mkdirs(new IgfsPath("/d/f"), null);

            fail("IgfsParentNotDirectoryException expected.");
        }
        catch (IgfsParentNotDirectoryException ignore) {
            // No-op.
        }
        catch (IgfsException ignore) {
            // Currently Ok for Hadoop fs:
            if (!getClass().getSimpleName().startsWith("Hadoop"))
                throw ignore;
        }

        try {
            igfs.mkdirs(new IgfsPath("/d/f/something/else"), null);

            fail("IgfsParentNotDirectoryException expected.");
        }
        catch (IgfsParentNotDirectoryException ignore) {
            // No-op.
        }
        catch (IgfsException ignore) {
            // Currently Ok for Hadoop fs:
            if (!getClass().getSimpleName().startsWith("Hadoop"))
                throw ignore;
        }

        create(igfs, paths(DIR, SUBDIR), null);

        igfs.mkdirs(SUBSUBDIR, props);

        // Ensure that directory was created and properties are propagated.
        checkExist(igfs, igfsSecondary, SUBSUBDIR);

        if (permissionsSupported()) {
            if (dual)
                // Check only permissions because user and group will always be present in Hadoop Fs.
                assertEquals(props.get(IgfsUtils.PROP_PERMISSION), igfsSecondary.permissions(SUBSUBDIR.toString()));

            // We check only permission because IGFS client adds username and group name explicitly.
            assertEquals(props.get(IgfsUtils.PROP_PERMISSION),
                igfs.info(SUBSUBDIR).properties().get(IgfsUtils.PROP_PERMISSION));
        }
    }

    /**
     * Test mkdirs in case parent is the root directory.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    public void testMkdirsParentRoot() throws Exception {
        Map<String, String> props = properties(null, null, "0555"); // mkdirs command doesn't propagate user info.

        igfs.mkdirs(DIR, props);

        checkExist(igfs, igfsSecondary, DIR);

        if (permissionsSupported()) {
            if (dual)
                // check permission only since Hadoop Fs will always have user and group:
                assertEquals(props.get(IgfsUtils.PROP_PERMISSION), igfsSecondary.permissions(DIR.toString()));

            // We check only permission because IGFS client adds username and group name explicitly.
            assertEquals(props.get(IgfsUtils.PROP_PERMISSION),
                igfs.info(DIR).properties().get(IgfsUtils.PROP_PERMISSION));
        }
    }

    /**
     * Test delete in case both local and remote file systems have the same folder structure.
     *
     * @throws Exception If failed.
     */
    public void testDelete() throws Exception {
        create(igfs, paths(DIR, SUBDIR, SUBSUBDIR), paths(FILE));

        igfs.delete(SUBDIR, true);

        checkNotExist(igfs, igfsSecondary, SUBDIR, SUBSUBDIR, FILE);
    }

    /**
     * Test delete when the path parent is the root.
     *
     * @throws Exception If failed.
     */
    public void testDeleteParentRoot() throws Exception {
        create(igfs, paths(DIR, SUBDIR, SUBSUBDIR), paths(FILE));

        igfs.delete(DIR, true);

        checkNotExist(igfs, igfsSecondary, DIR, SUBDIR, SUBSUBDIR, FILE);
    }

    /**
     * Ensure that delete will not be successful in non-empty directory when recursive flag is set to {@code false}.
     *
     * @throws Exception If failed.
     */
    public void testDeleteDirectoryNotEmpty() throws Exception {
        create(igfs, paths(DIR, SUBDIR, SUBSUBDIR), paths(FILE));
        checkExist(igfs, igfsSecondary, SUBDIR, SUBSUBDIR, FILE);

        try {
            boolean ok = igfs.delete(SUBDIR, false);

            assertFalse(ok);
        }
        catch (IgfsDirectoryNotEmptyException ignore) {
            // No-op, expected.
        }

        checkExist(igfs, igfsSecondary, SUBDIR, SUBSUBDIR, FILE);
    }

    /**
     * Test update in case both local and remote file systems have the same folder structure.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    public void testUpdate() throws Exception {
        if(!propertiesSupported())
            return;

        Map<String, String> props = properties("owner", "group", "0555");

        create(igfs, paths(DIR, SUBDIR), paths(FILE));

        igfs.update(FILE, props);

        if (dual)
            assertEquals(props, igfsSecondary.properties(FILE.toString()));

        assertEquals(props, igfs.info(FILE).properties());
    }

    /**
     * Test update when parent is the root.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    public void testUpdateParentRoot() throws Exception {
        if(!propertiesSupported())
            return;

        Map<String, String> props = properties("owner", "group", "0555");

        create(igfs, paths(DIR), null);

        igfs.update(DIR, props);

        if (dual)
            assertEquals(props, igfsSecondary.properties(DIR.toString()));

        assertEquals(props, igfs.info(DIR).properties());
    }

    /**
     * Check that exception is thrown in case the path being updated doesn't exist remotely.
     *
     * @throws Exception If failed.
     */
    public void testUpdatePathDoesNotExist() throws Exception {
        final Map<String, String> props = properties("owner", "group", "0555");

        assert igfs.update(SUBDIR, props) == null;

        checkNotExist(igfs, igfsSecondary, SUBDIR);
    }

    /**
     * Ensure that formatting is not propagated to the secondary file system.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    public void testFormat() throws Exception {
        final GridCacheAdapter<IgfsBlockKey, byte[]> dataCache = getDataCache(igfs);

        assert dataCache != null;

        int size0 = dataCache.size(new CachePeekMode[] {CachePeekMode.ALL});
        assert size0 == 0 : "Initial data cache size = " + size0;

        if (dual)
            create(igfsSecondary, paths(DIR, SUBDIR, DIR_NEW, SUBDIR_NEW), paths(FILE, FILE_NEW));

        create(igfs, paths(DIR, SUBDIR), paths(FILE));

        try (IgfsOutputStream os = igfs.create(FILE, true)) {
            os.write(new byte[10 * 1024 * 1024]);
        }

        if (dual)
            checkExist(igfsSecondary, DIR, SUBDIR, FILE, DIR_NEW, SUBDIR_NEW, FILE_NEW);

        checkExist(igfs, DIR, SUBDIR, FILE);

        assert igfs.info(FILE).length() == 10 * 1024 * 1024;

        assert dataCache.size(new CachePeekMode[] {CachePeekMode.ALL}) > 0;

        igfs.format();

        // Ensure format is not propagated to the secondary file system.
        if (dual) {
            checkExist(igfsSecondary, DIR, SUBDIR, FILE, DIR_NEW, SUBDIR_NEW, FILE_NEW);

            igfsSecondary.format();
        }

        // Ensure entries deletion in the primary file system.
        checkNotExist(igfs, DIR, SUBDIR, FILE);

        if (!GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                try {
                    return dataCache.size(new CachePeekMode[] {CachePeekMode.ALL}) == 0;
                } catch (IgniteCheckedException ice) {
                    throw new IgniteException(ice);
                }
            }
        }, 10_000)) {
            Iterable<? extends GridCacheEntryEx> entries = dataCache.allEntries();

            for (GridCacheEntryEx e: entries) {
                X.println("deleted = " + e.deleted());
                X.println("detached = " + e.detached());
                X.println("info = " + e.info());
                X.println("k = " + e.key() + ", v = " + e.valueBytes());
            }

            assert false;
        }
    }

    /**
     * Tests that root directory properties persist afetr the #format() operation.
     *
     * @throws Exception If failed.
     */
    public void testRootPropertiesPersistAfterFormat() throws Exception {
        if(!propertiesSupported())
            return;

        if (dual && !(igfsSecondaryFileSystem instanceof IgfsSecondaryFileSystemImpl)) {
            // In case of Hadoop dual mode only user name, group name, and permission properties are updated,
            // an arbitrary named property is just ignored:
            checkRootPropertyUpdate("foo", "moo", null);
            checkRootPropertyUpdate(IgfsUtils.PROP_PERMISSION, "0777", "0777");
        }
        else {
            checkRootPropertyUpdate("foo", "moo", "moo");
            checkRootPropertyUpdate(IgfsUtils.PROP_PERMISSION, "0777", "0777");
        }
    }

    /**
     *
     * @throws Exception
     */
    private void checkRootPropertyUpdate(String prop, String setVal, String expGetVal) throws Exception {
        final IgfsPath rootPath = new IgfsPath("/");

        igfs.update(rootPath, Collections.singletonMap(prop, setVal));

        igfs.format();

        IgfsFile file = igfs.info(rootPath);

        assert file != null;

        Map<String,String> props = file.properties();

        assertEquals(expGetVal, props.get(prop));
    }

    /**
     * Test regular file open.
     *
     * @throws Exception If failed.
     */
    public void testOpen() throws Exception {
        create(igfs, paths(DIR, SUBDIR), null);

        createFile(igfs.asSecondary(), FILE, true, chunk);

        checkFileContent(igfs, FILE, chunk);

        // Read again when the whole file is in memory.
        checkFileContent(igfs, FILE, chunk);
    }

    /**
     * Test file open in case it doesn't exist both locally and remotely.
     *
     * @throws Exception If failed.
     */
    public void testOpenDoesNotExist() throws Exception {
        igfsSecondary.delete(FILE.toString(), false);

        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgfsInputStream is = null;

                try {
                    is = igfs.open(FILE);
                } finally {
                    U.closeQuiet(is);
                }

                return null;
            }
        }, IgfsPathNotFoundException.class, "File not found: " + FILE);
    }

    /**
     * Test setTimes operation.
     *
     * @throws Exception If failed.
     */
    public void testSetTimes() throws Exception {
        createFile(igfs.asSecondary(), FILE, true, chunk);

        checkExist(igfs, igfsSecondary, DIR);
        checkExist(igfs, igfsSecondary, SUBDIR);
        checkExist(igfs, igfsSecondary, FILE);

        checkSetTimes(SUBDIR);
        checkSetTimes(FILE);

        try {
            igfs.setTimes(FILE2, Long.MAX_VALUE, Long.MAX_VALUE);

            fail("Exception is not thrown for missing file.");
        }
        catch (Exception ignore) {
            // No-op.
        }
    }

    /**
     * Check setTimes logic for path.
     *
     * @param path Path.
     * @throws Exception If failed.
     */
    private void checkSetTimes(IgfsPath path) throws Exception {
        if (timesSupported()) {

            IgfsFile info = igfs.info(path);
            T2<Long, Long> secondaryTimes = dual ? igfsSecondary.times(path.toString()) : null;

            assert info != null;

            // Change nothing.
            igfs.setTimes(path, -1, -1);

            IgfsFile newInfo = igfs.info(path);

            assert newInfo != null;

            assertEquals(info.accessTime(), newInfo.accessTime());
            assertEquals(info.modificationTime(), newInfo.modificationTime());

            if (dual) {
                T2<Long, Long> newSecondaryTimes = igfsSecondary.times(path.toString());

                assertEquals(secondaryTimes.get1(), newSecondaryTimes.get1());
                assertEquals(secondaryTimes.get2(), newSecondaryTimes.get2());
            }

            // Change only access time.
            igfs.setTimes(path, info.accessTime() + 1, -1);

            newInfo = igfs.info(path);

            assert newInfo != null;

            assertEquals(info.accessTime() + 1, newInfo.accessTime());
            assertEquals(info.modificationTime(), newInfo.modificationTime());

            if (dual) {
                T2<Long, Long> newSecondaryTimes = igfsSecondary.times(path.toString());

                assertEquals(newInfo.accessTime(), (long) newSecondaryTimes.get1());
                assertEquals(secondaryTimes.get2(), newSecondaryTimes.get2());
            }

            // Change only modification time.
            igfs.setTimes(path, -1, info.modificationTime() + 1);

            newInfo = igfs.info(path);

            assert newInfo != null;

            assertEquals(info.accessTime() + 1, newInfo.accessTime());
            assertEquals(info.modificationTime() + 1, newInfo.modificationTime());

            if (dual) {
                T2<Long, Long> newSecondaryTimes = igfsSecondary.times(path.toString());

                assertEquals(newInfo.accessTime(), (long) newSecondaryTimes.get1());
                assertEquals(newInfo.modificationTime(), (long) newSecondaryTimes.get2());
            }

            // Change both.
            igfs.setTimes(path, info.accessTime() + 2, info.modificationTime() + 2);

            newInfo = igfs.info(path);

            assert newInfo != null;

            assertEquals(info.accessTime() + 2, newInfo.accessTime());
            assertEquals(info.modificationTime() + 2, newInfo.modificationTime());

            if (dual) {
                T2<Long, Long> newSecondaryTimes = igfsSecondary.times(path.toString());

                assertEquals(newInfo.accessTime(), (long) newSecondaryTimes.get1());
                assertEquals(newInfo.modificationTime(), (long) newSecondaryTimes.get2());
            }
        }
    }

    /**
     * Test regular create.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings({"ConstantConditions", "EmptyTryBlock", "UnusedDeclaration"})
    public void testCreate() throws Exception {
        create(igfs, paths(DIR, SUBDIR), null);

        createFile(igfs.asSecondary(), FILE, true, chunk);

        checkFile(igfs, igfsSecondary, FILE, chunk);

        try (IgfsOutputStream os = igfs.create(new IgfsPath("/r"), false)) {
            checkExist(igfs, igfsSecondary, new IgfsPath("/r"));
            assert igfs.info(new IgfsPath("/r")).isFile();
        }

        try (IgfsOutputStream os = igfs.create(new IgfsPath("/k/l"), false)) {
            checkExist(igfs, igfsSecondary, new IgfsPath("/k/l"));
            assert igfs.info(new IgfsPath("/k/l")).isFile();
        }

        try {
            try (IgfsOutputStream os = igfs.create(new IgfsPath("/k/l"), false)) {}

            fail("Exception expected");
        } catch (IgniteException e) {
            // okay
        }

        checkExist(igfs, igfsSecondary, new IgfsPath("/k/l"));
        assert igfs.info(new IgfsPath("/k/l")).isFile();

        try {
            try (IgfsOutputStream os = igfs.create(new IgfsPath("/k/l/m"), true)) {}

            fail("Exception expected");
        } catch (IgniteException e) {
            // okay
        }
        checkNotExist(igfs, igfsSecondary, new IgfsPath("/k/l/m"));
        checkExist(igfs, igfsSecondary, new IgfsPath("/k/l"));
        assert igfs.info(new IgfsPath("/k/l")).isFile();

        try {
            try (IgfsOutputStream os = igfs.create(new IgfsPath("/k/l/m/n/o/p"), true)) {}

            fail("Exception expected");
        } catch (IgniteException e) {
            // okay
        }
        checkNotExist(igfs, igfsSecondary, new IgfsPath("/k/l/m"));
        checkExist(igfs, igfsSecondary, new IgfsPath("/k/l"));
        assert igfs.info(new IgfsPath("/k/l")).isFile();

        igfs.mkdirs(new IgfsPath("/x/y"), null);
        try {
            try (IgfsOutputStream os = igfs.create(new IgfsPath("/x/y"), true)) {}

            fail("Exception expected");
        } catch (IgniteException e) {
            // okay
        }

        checkExist(igfs, igfsSecondary, new IgfsPath("/x/y"));
        assert igfs.info(new IgfsPath("/x/y")).isDirectory();

        try (IgfsOutputStream os = igfs.create(new IgfsPath("/x/y/f"), false)) {
            assert igfs.info(new IgfsPath("/x/y/f")).isFile();
        }

        try (IgfsOutputStream os = igfs.create(new IgfsPath("/x/y/z/f"), false)) {
            assert igfs.info(new IgfsPath("/x/y/z/f")).isFile();
        }

        try (IgfsOutputStream os = igfs.create(new IgfsPath("/x/y/z/t/f"), false)) {
            assert igfs.info(new IgfsPath("/x/y/z/t/f")).isFile();
        }

        try (IgfsOutputStream os = igfs.create(new IgfsPath("/x/y/z/t/t2/t3/t4/t5/f"), false)) {
            assert igfs.info(new IgfsPath("/x/y/z/t/t2/t3/t4/t5/f")).isFile();
        }
    }

    /**
     * Test create when parent is the root.
     *
     * @throws Exception If failed.
     */
    public void testCreateParentRoot() throws Exception {
        IgfsPath file = new IgfsPath("/" + FILE.name());

        createFile(igfs.asSecondary(), file, true, chunk);

        checkFile(igfs, igfsSecondary, file, chunk);
    }

    /**
     * Test subsequent "create" commands on the same file without closing the output streams.
     *
     * @throws Exception If failed.
     */
    public void testCreateNoClose() throws Exception {
        create(igfs, paths(DIR, SUBDIR), null);

        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Override public Object call() throws Exception {
                IgfsOutputStream os1 = null;
                IgfsOutputStream os2 = null;

                try {
                    os1 = igfs.create(FILE, true);
                    os2 = igfs.create(FILE, true);
                } finally {
                    U.closeQuiet(os1);
                    U.closeQuiet(os2);
                }

                return null;
            }
        }, IgfsException.class, null);
    }

    /**
     * Test rename on the file when it was opened for write(create) and is not closed yet.
     *
     * @throws Exception If failed.
     */
    public void testCreateRenameNoClose() throws Exception {
        create(igfs, paths(DIR, SUBDIR), null);

        IgfsOutputStream os = null;

        try {
            os = igfs.create(FILE, true);

            igfs.rename(FILE, FILE2);

            os.close();
        }
        finally {
            U.closeQuiet(os);
        }
    }

    /**
     * Test rename on the file parent when it was opened for write(create) and is not closed yet.
     *
     * @throws Exception If failed.
     */
    public void testCreateRenameParentNoClose() throws Exception {
        create(igfs, paths(DIR, SUBDIR), null);

        IgfsOutputStream os = null;

        try {
            os = igfs.create(FILE, true);

            igfs.rename(SUBDIR, SUBDIR2);

            os.close();
        }
        finally {
            U.closeQuiet(os);
        }
    }

    /**
     * Test delete on the file when it was opened for write(create) and is not closed yet.
     *
     * @throws Exception If failed.
     */
    public void testCreateDeleteNoClose() throws Exception {
        create(igfs, paths(DIR, SUBDIR), null);

        IgfsOutputStream os = null;

        IgniteUuid id = null;

        try {
            os = igfs.create(FILE, false);

            id = igfs.context().meta().fileId(FILE);

            assert id != null;

            boolean del = igfs.delete(FILE, false);

            assertTrue(del);
            assertFalse(igfs.exists(FILE));
            // The id still exists in meta cache since
            // it is locked for writing and just moved to TRASH.
            // Delete worker cannot delete it for that reason:
            assertTrue(igfs.context().meta().exists(id));

            os.write(chunk);

            os.close();
        }
        finally {
            U.closeQuiet(os);
        }

        final IgniteUuid id0 = id;

        // Delete worker should delete the file once its output stream is finally closed:
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                try {
                    return !igfs.context().meta().exists(id0);
                }
                catch (IgniteCheckedException ice) {
                    throw new IgniteException(ice);
                }
            }
        }, 5_000L);
    }

    /**
     * Test delete on the file parent when it was opened for write(create) and is not closed yet.
     *
     * @throws Exception If failed.
     */
    public void testCreateDeleteParentNoClose() throws Exception {
        create(igfs, paths(DIR, SUBDIR), null);

        IgfsOutputStream os = null;

        IgniteUuid id = null;

        try {
            os = igfs.create(FILE, false);

            id = igfs.context().meta().fileId(FILE);

            assert id != null;

            boolean del = igfs.delete(SUBDIR, true);

            assertTrue(del);
            assertFalse(igfs.exists(FILE));
            // The id still exists in meta cache since
            // it is locked for writing and just moved to TRASH.
            // Delete worker cannot delete it for that reason:
            assertTrue(igfs.context().meta().exists(id));

            os.write(chunk);

            os.close();
        }
        finally {
            U.closeQuiet(os);
        }

        final IgniteUuid id0 = id;

        // Delete worker should delete the file once its output stream is finally closed:
        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                try {
                    return !igfs.context().meta().exists(id0);
                }
                catch (IgniteCheckedException ice) {
                    throw new IgniteException(ice);
                }
            }
        }, 5_000L);
    }

    /**
     * Test update on the file when it was opened for write(create) and is not closed yet.
     *
     * @throws Exception If failed.
     */
    public void testCreateUpdateNoClose() throws Exception {
        if(!propertiesSupported())
            return;

        Map<String, String> props = properties("owner", "group", "0555");

        create(igfs, paths(DIR, SUBDIR), null);

        IgfsOutputStream os = null;

        try {
            os = igfs.create(FILE, true);

            igfs.update(FILE, props);

            os.close();
        }
        finally {
            U.closeQuiet(os);
        }
    }

    /**
     * Checks simple write.
     *
     * @throws Exception On error.
     */
    public void testSimpleWrite() throws Exception {
        IgfsPath path = new IgfsPath("/file1");

        IgfsOutputStream os = igfs.create(path, 128, true/*overwrite*/, null, 0, 256, null);

        os.write(chunk);

        os.close();

        assert igfs.exists(path);
        checkFileContent(igfs, path, chunk);

        os = igfs.create(path, 128, true/*overwrite*/, null, 0, 256, null);

        assert igfs.exists(path);

        os.write(chunk);

        assert igfs.exists(path);

        os.write(chunk);

        assert igfs.exists(path);

        os.close();

        assert igfs.exists(path);
        checkFileContent(igfs, path, chunk, chunk);
    }

    /**
     * Ensure consistency of data during file creation.
     *
     * @throws Exception If failed.
     */
    public void testCreateConsistency() throws Exception {
        final AtomicInteger ctr = new AtomicInteger();
        final AtomicReference<Exception> err = new AtomicReference<>();

        final int threadCnt = 10;

        multithreaded(new Runnable() {
            @Override public void run() {
                int idx = ctr.incrementAndGet();

                final IgfsPath path = new IgfsPath("/file" + idx);

                try {
                    for (int i = 0; i < REPEAT_CNT; i++) {
                        IgfsOutputStream os = igfs.create(path, 128, true/*overwrite*/, null, 0, 256, null);

                        os.write(chunk);

                        os.close();

                        assert igfs.exists(path);
                    }

                    awaitFileClose(igfs.asSecondary(), path);

                    checkFileContent(igfs, path, chunk);
                }
                catch (IOException | IgniteCheckedException e) {
                    err.compareAndSet(null, e); // Log the very first error.
                }
            }
        }, threadCnt);

        if (err.get() != null)
            throw err.get();
    }

    /**
     * Ensure create consistency when multiple threads writes to the same file.
     *
     * @throws Exception If failed.
     */
    public void testCreateConsistencyMultithreaded() throws Exception {
        // TODO: Enable
//        final AtomicBoolean stop = new AtomicBoolean();
//
//        final AtomicInteger createCtr = new AtomicInteger(); // How many times the file was re-created.
//        final AtomicReference<Exception> err = new AtomicReference<>();
//
//        igfs.create(FILE, false).close();
//
//        int threadCnt = 50;
//
//        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
//            @SuppressWarnings("ThrowFromFinallyBlock")
//            @Override public void run() {
//                while (!stop.get() && err.get() == null) {
//                    IgfsOutputStream os = null;
//
//                    try {
//                        os = igfs.create(FILE, true);
//
//                        os.write(chunk);
//
//                        os.close();
//
//                        createCtr.incrementAndGet();
//                    }
//                    catch (IgniteException e) {
//                        // No-op.
//                    }
//                    catch (IOException e) {
//                        err.compareAndSet(null, e);
//
//                        Throwable[] chain = X.getThrowables(e);
//
//                        Throwable cause = chain[chain.length - 1];
//
//                        System.out.println("Failed due to IOException exception. Cause:");
//                        cause.printStackTrace(System.out);
//                    }
//                    finally {
//                        if (os != null)
//                            try {
//                                os.close();
//                            }
//                            catch (IOException ioe) {
//                                throw new IgniteException(ioe);
//                            }
//                    }
//                }
//            }
//        }, threadCnt);
//
//        long startTime = U.currentTimeMillis();
//
//        while (err.get() == null
//                && createCtr.get() < 500
//                && U.currentTimeMillis() - startTime < 60 * 1000)
//            U.sleep(100);
//
//        stop.set(true);
//
//        fut.get();
//
//        awaitFileClose(igfs.asSecondary(), FILE);
//
//        if (err.get() != null) {
//            X.println("Test failed: rethrowing first error: " + err.get());
//
//            throw err.get();
//        }
//
//        checkFileContent(igfs, FILE, chunk);
    }

    /**
     * Test regular append.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings({"TryFinallyCanBeTryWithResources", "EmptyTryBlock"})
    public void testAppend() throws Exception {
        if (appendSupported()) {
            create(igfs, paths(DIR, SUBDIR), null);

            assert igfs.exists(SUBDIR);

            createFile(igfs, FILE, true, BLOCK_SIZE, chunk);

            checkFile(igfs, igfsSecondary, FILE, chunk);

            appendFile(igfs, FILE, chunk);

            checkFile(igfs, igfsSecondary, FILE, chunk, chunk);

            // Test create via append:
            IgfsPath path2 = FILE2;

            IgfsOutputStream os = null;

            try {
                os = igfs.append(path2, true/*create*/);

                writeFileChunks(os, chunk);
            } finally {
                U.closeQuiet(os);

                awaitFileClose(igfs.asSecondary(), path2);
            }

            try {
                os = igfs.append(path2, false/*create*/);

                writeFileChunks(os, chunk);
            } finally {
                U.closeQuiet(os);

                awaitFileClose(igfs.asSecondary(), path2);
            }

            checkFile(igfs, igfsSecondary, path2, chunk, chunk);

            // Negative append (create == false):
            try {
                try (IgfsOutputStream ignored = igfs.append(new IgfsPath("/should-not-be-created"), false)) {
                    // No-op.
                }

                fail("Exception expected");
            } catch (IgniteException e) {
                // okay
            }
            checkNotExist(igfs, igfsSecondary, new IgfsPath("/d1"));

            // Positive mkdirs via append:
            try (IgfsOutputStream ignored = igfs.append(new IgfsPath("/k/l"), true)) {
                checkExist(igfs, igfsSecondary, new IgfsPath("/k/l"));
                assert igfs.info(new IgfsPath("/k/l")).isFile();
            }

            // Negative append (file is immediate parent):
            try {
                try (IgfsOutputStream ignored = igfs.append(new IgfsPath("/k/l/m"), true)) {
                    // No-op.
                }

                fail("Exception expected");
            } catch (IgniteException e) {
                // okay
            }
            checkNotExist(igfs, igfsSecondary, new IgfsPath("/k/l/m"));
            checkExist(igfs, igfsSecondary, new IgfsPath("/k/l"));
            assert igfs.info(new IgfsPath("/k/l")).isFile();

            // Negative append (file is in the parent chain):
            try {
                try (IgfsOutputStream ignored = igfs.append(new IgfsPath("/k/l/m/n/o/p"), true)) {
                    // No-op.
                }

                fail("Exception expected");
            } catch (IgniteException e) {
                // okay
            }
            checkNotExist(igfs, igfsSecondary, new IgfsPath("/k/l/m"));
            checkExist(igfs, igfsSecondary, new IgfsPath("/k/l"));
            assert igfs.info(new IgfsPath("/k/l")).isFile();

            // Negative append (target is a directory):
            igfs.mkdirs(new IgfsPath("/x/y"), null);
            checkExist(igfs, igfsSecondary, new IgfsPath("/x/y"));
            assert igfs.info(new IgfsPath("/x/y")).isDirectory();
            try {
                try (IgfsOutputStream ignored = igfs.append(new IgfsPath("/x/y"), true)) {
                    // No-op.
                }

                fail("Exception expected");
            } catch (IgniteException e) {
                // okay
            }

            // Positive append with create
            try (IgfsOutputStream ignored = igfs.append(new IgfsPath("/x/y/f"), true)) {
                assert igfs.info(new IgfsPath("/x/y/f")).isFile();
            }

            // Positive append with create & 1 mkdirs:
            try (IgfsOutputStream ignored = igfs.append(new IgfsPath("/x/y/z/f"), true)) {
                assert igfs.info(new IgfsPath("/x/y/z/f")).isFile();
            }

            // Positive append with create & 2 mkdirs:
            try (IgfsOutputStream ignored = igfs.append(new IgfsPath("/x/y/z/t/f"), true)) {
                assert igfs.info(new IgfsPath("/x/y/z/t/f")).isFile();
            }

            // Positive mkdirs create & many mkdirs:
            try (IgfsOutputStream ignored = igfs.append(new IgfsPath("/x/y/z/t/t2/t3/t4/t5/f"), true)) {
                assert igfs.info(new IgfsPath("/x/y/z/t/t2/t3/t4/t5/f")).isFile();
            }

            // Negative mkdirs via append (create == false):
            try {
                try (IgfsOutputStream ignored = igfs.append(new IgfsPath("/d1/d2/d3/f"), false)) {
                    // No-op.
                }

                fail("Exception expected");
            } catch (IgniteException e) {
                // okay
            }
            checkNotExist(igfs, igfsSecondary, new IgfsPath("/d1"));
        }
    }

    /**
     * Test create when parent is the root.
     *
     * @throws Exception If failed.
     */
    public void testAppendParentRoot() throws Exception {
        if (appendSupported()) {
            IgfsPath file = new IgfsPath("/" + FILE.name());

            createFile(igfs, file, true, BLOCK_SIZE, chunk);

            appendFile(igfs, file, chunk);

            checkFile(igfs, igfsSecondary, file, chunk, chunk);
        }
    }

    /**
     * Test subsequent "append" commands on the same file without closing the output streams.
     *
     * @throws Exception If failed.
     */
    public void testAppendNoClose() throws Exception {
        if (appendSupported()) {
            create(igfs, paths(DIR, SUBDIR), null);

            createFile(igfs.asSecondary(), FILE, false);

            GridTestUtils.assertThrowsInherited(log(), new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    IgfsOutputStream os1 = null;
                    IgfsOutputStream os2 = null;

                    try {
                        os1 = igfs.append(FILE, false);
                        os2 = igfs.append(FILE, false);
                    } finally {
                        U.closeQuiet(os1);
                        U.closeQuiet(os2);
                    }

                    return null;
                }
            }, IgniteException.class, null);
        }
    }

    /**
     * Test rename on the file when it was opened for write(append) and is not closed yet.
     *
     * @throws Exception If failed.
     */
    public void testAppendRenameNoClose() throws Exception {
        if (appendSupported()) {
            create(igfs, paths(DIR, SUBDIR), null);

            createFile(igfs.asSecondary(), FILE, false);

            IgfsOutputStream os = null;

            try {
                os = igfs.append(FILE, false);

                igfs.rename(FILE, FILE2);

                os.close();
            } finally {
                U.closeQuiet(os);
            }
        }
    }

    /**
     * Test rename on the file parent when it was opened for write(append) and is not closed yet.
     *
     * @throws Exception If failed.
     */
    public void testAppendRenameParentNoClose() throws Exception {
        if (appendSupported()) {
            create(igfs.asSecondary(), paths(DIR, SUBDIR), null);

            createFile(igfs.asSecondary(), FILE, false);

            IgfsOutputStream os = null;

            try {
                os = igfs.append(FILE, false);

                igfs.rename(SUBDIR, SUBDIR2);

                os.close();
            } finally {
                U.closeQuiet(os);
            }
        }
    }

    /**
     * Test delete on the file when it was opened for write(append) and is not closed yet.
     *
     * @throws Exception If failed.
     */
    public void testAppendDeleteNoClose() throws Exception {
        if (appendSupported()) {
            create(igfs, paths(DIR, SUBDIR), null);

            createFile(igfs.asSecondary(), FILE, false);

            IgfsOutputStream os = null;
            IgniteUuid id = null;

            try {
                id = igfs.context().meta().fileId(FILE);

                os = igfs.append(FILE, false);

                boolean del = igfs.delete(FILE, false);

                assertTrue(del);
                assertFalse(igfs.exists(FILE));
                assertTrue(igfs.context().meta().exists(id)); // id still exists in meta cache since
                // it is locked for writing and just moved to TRASH.
                // Delete worker cannot delete it for that reason.

                os.write(chunk);

                os.close();
            } finally {
                U.closeQuiet(os);
            }

            assert id != null;

            final IgniteUuid id0 = id;

            // Delete worker should delete the file once its output stream is finally closed:
            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override
                public boolean apply() {
                    try {
                        return !igfs.context().meta().exists(id0);
                    } catch (IgniteCheckedException ice) {
                        throw new IgniteException(ice);
                    }
                }
            }, 5_000L);
        }
    }

    /**
     * Test delete on the file parent when it was opened for write(append) and is not closed yet.
     *
     * @throws Exception If failed.
     */
    public void testAppendDeleteParentNoClose() throws Exception {
        if (appendSupported()) {
            create(igfs, paths(DIR, SUBDIR), null);

            createFile(igfs.asSecondary(), FILE, false);

            IgfsOutputStream os = null;
            IgniteUuid id = null;

            try {
                id = igfs.context().meta().fileId(FILE);

                os = igfs.append(FILE, false);

                boolean del = igfs.delete(SUBDIR, true); // Since GG-4911 we allow deletes in this case.

                assertTrue(del);
                assertFalse(igfs.exists(FILE));
                assertTrue(igfs.context().meta().exists(id)); // id still exists in meta cache since
                // it is locked for writing and just moved to TRASH.
                // Delete worker cannot delete it for that reason.

                os.write(chunk);

                os.close();
            } finally {
                U.closeQuiet(os);
            }

            assert id != null;

            final IgniteUuid id0 = id;

            // Delete worker should delete the file once its output stream is finally closed:
            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override
                public boolean apply() {
                    try {
                        return !igfs.context().meta().exists(id0);
                    } catch (IgniteCheckedException ice) {
                        throw new IgniteException(ice);
                    }
                }
            }, 5_000L);
        }
    }

    /**
     * Test update on the file when it was opened for write(create) and is not closed yet.
     *
     * @throws Exception If failed.
     */
    public void testAppendUpdateNoClose() throws Exception {
        if (appendSupported()) {
            Map<String, String> props = properties("owner", "group", "0555");

            create(igfs, paths(DIR, SUBDIR), null);

            createFile(igfs.asSecondary(), FILE, false);

            IgfsOutputStream os = null;

            try {
                os = igfs.append(FILE, false);

                if (permissionsSupported())
                    igfs.update(FILE, props);

                os.close();
            } finally {
                U.closeQuiet(os);
            }
        }
    }

    /**
     * Ensure consistency of data during appending to a file.
     *
     * @throws Exception If failed.
     */
    public void testAppendConsistency() throws Exception {
        if (appendSupported()) {
            final AtomicInteger ctr = new AtomicInteger();
            final AtomicReference<Exception> err = new AtomicReference<>();

            int threadCnt = 10;

            for (int i = 0; i < threadCnt; i++)
                createFile(igfs.asSecondary(), new IgfsPath("/file" + i), false);

            multithreaded(new Runnable() {
                @Override
                public void run() {
                    int idx = ctr.getAndIncrement();

                    IgfsPath path = new IgfsPath("/file" + idx);

                    try {
                        byte[][] chunks = new byte[REPEAT_CNT][];

                        for (int i = 0; i < REPEAT_CNT; i++) {
                            chunks[i] = chunk;

                            IgfsOutputStream os = igfs.append(path, false);

                            os.write(chunk);

                            os.close();

                            assert igfs.exists(path);
                        }

                        awaitFileClose(igfs.asSecondary(), path);

                        checkFileContent(igfs, path, chunks);
                    } catch (IOException | IgniteCheckedException e) {
                        err.compareAndSet(null, e); // Log the very first error.
                    }
                }
            }, threadCnt);

            if (err.get() != null)
                throw err.get();
        }
    }

    /**
     * Ensure append consistency when multiple threads writes to the same file.
     *
     * @throws Exception If failed.
     */
    public void testAppendConsistencyMultithreaded() throws Exception {
        if (appendSupported()) {
            final AtomicBoolean stop = new AtomicBoolean();

            final AtomicInteger chunksCtr = new AtomicInteger(); // How many chunks were written.
            final AtomicReference<Exception> err = new AtomicReference<>();

            igfs.create(FILE, false).close();

            int threadCnt = 50;

            IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
                @SuppressWarnings("ThrowFromFinallyBlock")
                @Override
                public void run() {
                    while (!stop.get() && err.get() == null) {
                        IgfsOutputStream os = null;

                        try {
                            os = igfs.append(FILE, false);

                            os.write(chunk);

                            os.close();

                            chunksCtr.incrementAndGet();
                        } catch (IgniteException ignore) {
                            // No-op.
                        } catch (IOException e) {
                            err.compareAndSet(null, e);
                        } finally {
                            if (os != null)
                                try {
                                    os.close();
                                } catch (IOException ioe) {
                                    throw new IgniteException(ioe);
                                }
                        }
                    }
                }
            }, threadCnt);

            long startTime = U.currentTimeMillis();

            while (err.get() == null
                && chunksCtr.get() < 50 && U.currentTimeMillis() - startTime < 60 * 1000)
                U.sleep(100);

            stop.set(true);

            fut.get();

            awaitFileClose(igfs.asSecondary(), FILE);

            if (err.get() != null) {
                X.println("Test failed: rethrowing first error: " + err.get());

                throw err.get();
            }

            byte[][] data = new byte[chunksCtr.get()][];

            Arrays.fill(data, chunk);

            checkFileContent(igfs, FILE, data);
        }
    }

    /**
     * Ensure that IGFS is able to stop in case not closed output stream exist.
     *
     * @throws Exception If failed.
     */
    public void testStop() throws Exception {
        create(igfs, paths(DIR, SUBDIR), null);

        IgfsOutputStream os = igfs.create(FILE, true);

        os.write(chunk);

        igfs.stop(true);

        // Reset test state.
        afterTestsStopped();
        beforeTestsStarted();
    }

    /**
     * Ensure that in case we create the folder A and delete its parent at the same time, resulting file system
     * structure is consistent.
     *
     * @throws Exception If failed.
     */
    public void testConcurrentMkdirsDelete() throws Exception {
        for (int i = 0; i < REPEAT_CNT; i++) {
            final CyclicBarrier barrier = new CyclicBarrier(2);

            IgniteInternalFuture<Boolean> res1 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    try {
                        igfs.mkdirs(SUBSUBDIR);
                    }
                    catch (IgniteException ignored) {
                        return false;
                    }

                    return true;
                }
            });

            IgniteInternalFuture<Boolean> res2 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    try {
                        return igfs.delete(DIR, true);
                    }
                    catch (IgniteException ignored) {
                        return false;
                    }
                }
            });

            assert res1.get(); // MKDIRS must succeed anyway.

            if (res2.get())
                checkNotExist(igfs, igfsSecondary, DIR, SUBDIR, SUBSUBDIR);
            else
                checkExist(igfs, igfsSecondary, DIR, SUBDIR, SUBSUBDIR);

            clear(igfs, igfsSecondary);
        }
    }

    /**
     * Ensure that in case we rename the folder A and delete it at the same time, only one of these requests succeed.
     *
     * @throws Exception If failed.
     */
    public void testConcurrentRenameDeleteSource() throws Exception {
        for (int i = 0; i < REPEAT_CNT; i++) {
            final CyclicBarrier barrier = new CyclicBarrier(2);

            create(igfs, paths(DIR, SUBDIR, DIR_NEW), paths());

            IgniteInternalFuture<Boolean> res1 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    try {
                        igfs.rename(SUBDIR, SUBDIR_NEW);

                        return true;
                    }
                    catch (IgniteException ignored) {
                        return false;
                    }
                }
            });

            IgniteInternalFuture<Boolean> res2 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    try {
                        return igfs.delete(SUBDIR, true);
                    }
                    catch (IgniteException ignored) {
                        return false;
                    }
                }
            });

            res1.get();
            res2.get();

            if (res1.get()) {
                assert !res2.get(); // Rename succeeded, so delete must fail.

                checkExist(igfs, igfsSecondary, DIR, DIR_NEW, SUBDIR_NEW);
                checkNotExist(igfs, igfsSecondary, SUBDIR);
            }
            else {
                assert res2.get(); // Rename failed because delete succeeded.

                checkExist(igfs, DIR); // DIR_NEW should not be synchronized with he primary IGFS.

                if (dual)
                    checkExist(igfsSecondary, DIR, DIR_NEW);

                checkNotExist(igfs, igfsSecondary, SUBDIR, SUBDIR_NEW);
            }

            clear(igfs, igfsSecondary);
        }
    }

    /**
     * Ensure that in case we rename the folder A to B and delete B at the same time, FS consistency is not
     * compromised.
     *
     * @throws Exception If failed.
     */
    public void testConcurrentRenameDeleteDestination() throws Exception {
        for (int i = 0; i < REPEAT_CNT; i++) {
            final CyclicBarrier barrier = new CyclicBarrier(2);

            create(igfs, paths(DIR, SUBDIR, DIR_NEW), paths());

            IgniteInternalFuture<Boolean> res1 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    try {
                        igfs.rename(SUBDIR, SUBDIR_NEW);

                        return true;
                    }
                    catch (IgniteException ignored) {
                        return false;
                    }
                }
            });

            IgniteInternalFuture<Boolean> res2 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    try {
                        return igfs.delete(SUBDIR_NEW, true);
                    }
                    catch (IgniteException ignored) {
                        return false;
                    }
                }
            });

            assert res1.get();

            if (res2.get()) {
                // Delete after rename.
                checkExist(igfs, igfsSecondary, DIR, DIR_NEW);
                checkNotExist(igfs, igfsSecondary, SUBDIR, SUBDIR_NEW);
            }
            else {
                // Delete before rename.
                checkExist(igfs, igfsSecondary, DIR, DIR_NEW, SUBDIR_NEW);
                checkNotExist(igfs, igfsSecondary, SUBDIR);
            }

            clear(igfs, igfsSecondary);
        }
    }

    /**
     * Ensure file system consistency in case two concurrent rename requests are executed: A -> B and B -> A.
     *
     * @throws Exception If failed.
     */
    public void testConcurrentRenames() throws Exception {
        for (int i = 0; i < REPEAT_CNT; i++) {
            final CyclicBarrier barrier = new CyclicBarrier(2);

            create(igfs, paths(DIR, SUBDIR, DIR_NEW), paths());

            IgniteInternalFuture<Boolean> res1 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    try {
                        igfs.rename(SUBDIR, SUBDIR_NEW);

                        return true;
                    }
                    catch (IgniteException ignored) {
                        return false;
                    }
                }
            });

            IgniteInternalFuture<Boolean> res2 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    try {
                        igfs.rename(SUBDIR_NEW, SUBDIR);

                        return true;
                    }
                    catch (IgniteException ignored) {
                        return false;
                    }
                }
            });

            res1.get();
            res2.get();

            assert res1.get(); // First rename must be successful anyway.

            if (res2.get()) {
                checkExist(igfs, igfsSecondary, DIR, SUBDIR, DIR_NEW);
                checkNotExist(igfs, igfsSecondary, SUBDIR_NEW);
            }
            else {
                checkExist(igfs, igfsSecondary, DIR, DIR_NEW, SUBDIR_NEW);
                checkNotExist(igfs, igfsSecondary, SUBDIR);
            }

            clear(igfs, igfsSecondary);
        }
    }

    /**
     * Ensure that in case we delete the folder A and delete its parent at the same time, resulting file system
     * structure is consistent.
     *
     * @throws Exception If failed.
     */
    public void testConcurrentDeletes() throws Exception {
        for (int i = 0; i < REPEAT_CNT; i++) {
            final CyclicBarrier barrier = new CyclicBarrier(2);

            create(igfs, paths(DIR, SUBDIR, SUBSUBDIR), paths());

            IgniteInternalFuture<Boolean> res1 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    try {
                        igfs.delete(SUBDIR, true);

                        return true;
                    }
                    catch (IgniteException ignored) {
                        return false;
                    }
                }
            });

            IgniteInternalFuture<Boolean> res2 = execute(new Callable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    U.awaitQuiet(barrier);

                    try {
                        igfs.delete(SUBSUBDIR, true);

                        return true;
                    }
                    catch (IgniteException ignored) {
                        return false;
                    }
                }
            });

            assert res1.get(); // Delete on the parent must succeed anyway.
            res2.get();

            checkExist(igfs, igfsSecondary, DIR);
            checkNotExist(igfs, igfsSecondary, SUBDIR, SUBSUBDIR);

            clear(igfs, igfsSecondary);
        }
    }

    /**
     * Ensure that deadlocks do not occur during concurrent rename operations.
     *
     * @throws Exception If failed.
     */
    public void testDeadlocksRename() throws Exception {
        checkDeadlocksRepeat(5, 2, 2, 2,  RENAME_CNT, 0, 0, 0, 0);
    }

    /**
     * Ensure that deadlocks do not occur during concurrent delete operations.
     *
     * @throws Exception If failed.
     */
    public void testDeadlocksDelete() throws Exception {
         checkDeadlocksRepeat(5, 2, 2, 2,  0, DELETE_CNT, 0, 0, 0);
    }

    /**
     * Ensure that deadlocks do not occur during concurrent update operations.
     *
     * @throws Exception If failed.
     */
    public void testDeadlocksUpdate() throws Exception {
        checkDeadlocksRepeat(5, 2, 2, 2, 0, 0, UPDATE_CNT, 0, 0);
    }

    /**
     * Ensure that deadlocks do not occur during concurrent directory creation operations.
     *
     * @throws Exception If failed.
     */
    public void testDeadlocksMkdirs() throws Exception {
         checkDeadlocksRepeat(5, 2, 2, 2,  0, 0, 0, MKDIRS_CNT, 0);
    }

    /**
     * Ensure that deadlocks do not occur during concurrent delete & rename operations.
     *
     * @throws Exception If failed.
     */
    public void testDeadlocksDeleteRename() throws Exception {
        checkDeadlocksRepeat(5, 2, 2, 2,  RENAME_CNT, DELETE_CNT, 0, 0, 0);
    }

    /**
     * Ensure that deadlocks do not occur during concurrent delete & rename operations.
     *
     * @throws Exception If failed.
     */
    public void testDeadlocksDeleteMkdirsRename() throws Exception {
        checkDeadlocksRepeat(5, 2, 2, 2,  RENAME_CNT, DELETE_CNT, 0, MKDIRS_CNT, 0);
    }

    /**
     * Ensure that deadlocks do not occur during concurrent delete & rename operations.
     *
     * @throws Exception If failed.
     */
    public void testDeadlocksDeleteMkdirs() throws Exception {
        checkDeadlocksRepeat(5, 2, 2, 2,  0, DELETE_CNT, 0, MKDIRS_CNT, 0);
    }

    /**
     * Ensure that deadlocks do not occur during concurrent file creation operations.
     *
     * @throws Exception If failed.
     */
    public void testDeadlocksCreate() throws Exception {
        checkDeadlocksRepeat(5, 2, 2, 2, 0, 0, 0, 0, CREATE_CNT);
    }

    /**
     * Ensure that deadlocks do not occur during concurrent operations of various types.
     *
     * @throws Exception If failed.
     */
    public void testDeadlocks() throws Exception {
        checkDeadlocksRepeat(5, 2, 2, 2,  RENAME_CNT, DELETE_CNT, UPDATE_CNT, MKDIRS_CNT, CREATE_CNT);
    }

    /**
     * Invokes {@link #checkDeadlocks(int, int, int, int, int, int, int, int, int)} for
     *  {@link #REPEAT_CNT} times.
     *
     * @param lvlCnt Total levels in folder hierarchy.
     * @param childrenDirPerLvl How many children directories to create per level.
     * @param childrenFilePerLvl How many children file to create per level.
     * @param primaryLvlCnt How many levels will exist in the primary file system before check start.
     * @param renCnt How many renames to perform.
     * @param delCnt How many deletes to perform.
     * @param updateCnt How many updates to perform.
     * @param mkdirsCnt How many directory creations to perform.
     * @param createCnt How many file creations to perform.
     * @throws Exception If failed.
     */
    private void checkDeadlocksRepeat(final int lvlCnt, final int childrenDirPerLvl, final int childrenFilePerLvl,
        int primaryLvlCnt, int renCnt, int delCnt,
        int updateCnt, int mkdirsCnt, int createCnt) throws Exception {
        // TODO: Enable.
//        if (relaxedConsistency())
//            return;
//
//        for (int i = 0; i < REPEAT_CNT; i++) {
//            try {
//                checkDeadlocks(lvlCnt, childrenDirPerLvl, childrenFilePerLvl, primaryLvlCnt, renCnt, delCnt,
//                    updateCnt, mkdirsCnt, createCnt);
//
//                if (i % 10 == 0)
//                    X.println(" - " + i);
//            }
//            finally {
//                clear(igfs, igfsSecondary);
//            }
//        }
    }

    /**
     * Check deadlocks by creating complex directories structure and then executing chaotic operations on it. A lot of
     * exception are expected here. We are not interested in them. Instead, we want to ensure that no deadlocks occur
     * during execution.
     *
     * @param lvlCnt Total levels in folder hierarchy.
     * @param childrenDirPerLvl How many children directories to create per level.
     * @param childrenFilePerLvl How many children file to create per level.
     * @param primaryLvlCnt How many levels will exist in the primary file system before check start.
     * @param renCnt How many renames to perform.
     * @param delCnt How many deletes to perform.
     * @param updateCnt How many updates to perform.
     * @param mkdirsCnt How many directory creations to perform.
     * @param createCnt How many file creations to perform.
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    private void checkDeadlocks(final int lvlCnt, final int childrenDirPerLvl, final int childrenFilePerLvl,
        int primaryLvlCnt, int renCnt, int delCnt, int updateCnt, int mkdirsCnt, int createCnt) throws Exception {
        assert childrenDirPerLvl > 0;

        // First define file system structure.
        final Map<Integer, List<IgfsPath>> dirPaths = new HashMap<>();
        final Map<Integer, List<IgfsPath>> filePaths = new HashMap<>();

        Queue<IgniteBiTuple<Integer, IgfsPath>> queue = new ArrayDeque<>();

        queue.add(F.t(0, new IgfsPath())); // Add root directory.

        while (!queue.isEmpty()) {
            IgniteBiTuple<Integer, IgfsPath> entry = queue.poll();

            int lvl = entry.getKey();

            if (lvl < lvlCnt) {
                int newLvl = lvl + 1;

                for (int i = 0; i < childrenDirPerLvl; i++) {
                    IgfsPath path = new IgfsPath(entry.getValue(), "dir-" + newLvl + "-" + i);

                    queue.add(F.t(newLvl, path));

                    if (!dirPaths.containsKey(newLvl))
                        dirPaths.put(newLvl, new ArrayList<IgfsPath>());

                    dirPaths.get(newLvl).add(path);
                }

                for (int i = 0; i < childrenFilePerLvl; i++) {
                    IgfsPath path = new IgfsPath(entry.getValue(), "file-" + newLvl + "-" + i);

                    if (!filePaths.containsKey(newLvl))
                        filePaths.put(newLvl, new ArrayList<IgfsPath>());

                    filePaths.get(newLvl).add(path);
                }
            }
        }

        // Now as we have all paths defined, plan operations on them.
        final Random rand = new Random(SEED);

        final int totalOpCnt = renCnt + delCnt + updateCnt + mkdirsCnt + createCnt;

        if (totalOpCnt == 0)
            throw new RuntimeException("Operations count is zero.");

        final CyclicBarrier barrier = new CyclicBarrier(totalOpCnt);

        Collection<Thread> threads = new ArrayList<>(totalOpCnt);

        // Renames.
        for (int i = 0; i < renCnt; i++) {
            Runnable r = new Runnable() {
                @Override public void run() {
                    try {
                        int fromLvl = rand.nextInt(lvlCnt) + 1;
                        int toLvl = rand.nextInt(lvlCnt) + 1;

                        List<IgfsPath> fromPaths;
                        List<IgfsPath> toPaths;

                        if (rand.nextInt(childrenDirPerLvl + childrenFilePerLvl) < childrenDirPerLvl) {
                            // Rename directories.
                            fromPaths = dirPaths.get(fromLvl);
                            toPaths = dirPaths.get(toLvl);
                        }
                        else {
                            // Rename files.
                            fromPaths = filePaths.get(fromLvl);
                            toPaths = filePaths.get(toLvl);
                        }

                        IgfsPath fromPath = fromPaths.get(rand.nextInt(fromPaths.size()));
                        IgfsPath toPath = toPaths.get(rand.nextInt(toPaths.size()));

                        U.awaitQuiet(barrier);

                        igfs.rename(fromPath, toPath);
                    }
                    catch (IgniteException ignore) {
                        // No-op.
                    }
                }
            };

            threads.add(new Thread(r));
        }

        // Deletes.
        for (int i = 0; i < delCnt; i++) {
            Runnable r = new Runnable() {
                @Override public void run() {
                    try {
                        int lvl = rand.nextInt(lvlCnt) + 1;

                        IgfsPath path = rand.nextInt(childrenDirPerLvl + childrenFilePerLvl) < childrenDirPerLvl ?
                            dirPaths.get(lvl).get(rand.nextInt(dirPaths.get(lvl).size())) :
                            filePaths.get(lvl).get(rand.nextInt(filePaths.get(lvl).size()));

                        U.awaitQuiet(barrier);

                        igfs.delete(path, true);
                    }
                    catch (IgniteException ignore) {
                        // No-op.
                    }
                }
            };

            threads.add(new Thread(r));
        }

        // Updates.
        for (int i = 0; i < updateCnt; i++) {
            Runnable r = new Runnable() {
                @Override public void run() {
                    try {
                        int lvl = rand.nextInt(lvlCnt) + 1;

                        IgfsPath path = rand.nextInt(childrenDirPerLvl + childrenFilePerLvl) < childrenDirPerLvl ?
                            dirPaths.get(lvl).get(rand.nextInt(dirPaths.get(lvl).size())) :
                            filePaths.get(lvl).get(rand.nextInt(filePaths.get(lvl).size()));

                        U.awaitQuiet(barrier);

                        igfs.update(path, properties("owner", "group", null));
                    }
                    catch (IgniteException ignore) {
                        // No-op.
                    }
                }
            };

            threads.add(new Thread(r));
        }

        // Directory creations.
        final AtomicInteger dirCtr = new AtomicInteger();

        for (int i = 0; i < mkdirsCnt; i++) {
            Runnable r = new Runnable() {
                @Override public void run() {
                    try {
                        int lvl = rand.nextInt(lvlCnt) + 1;

                        IgfsPath parentPath = dirPaths.get(lvl).get(rand.nextInt(dirPaths.get(lvl).size()));

                        IgfsPath path = new IgfsPath(parentPath, "newDir-" + dirCtr.incrementAndGet());

                        U.awaitQuiet(barrier);

                        igfs.mkdirs(path);

                    }
                    catch (IgniteException ignore) {
                        // No-op.
                    }
                }
            };

            threads.add(new Thread(r));
        }

        // File creations.
        final AtomicInteger fileCtr = new AtomicInteger();

        for (int i = 0; i < createCnt; i++) {
            Runnable r = new Runnable() {
                @Override public void run() {
                    try {
                        int lvl = rand.nextInt(lvlCnt) + 1;

                        IgfsPath parentPath = dirPaths.get(lvl).get(rand.nextInt(dirPaths.get(lvl).size()));

                        IgfsPath path = new IgfsPath(parentPath, "newFile-" + fileCtr.incrementAndGet());

                        U.awaitQuiet(barrier);

                        IgfsOutputStream os = null;

                        try {
                            os = igfs.create(path, true);

                            os.write(chunk);
                        }
                        finally {
                            U.closeQuiet(os);
                        }
                    }
                    catch (IOException | IgniteException ignore) {
                        // No-op.
                    }
                }
            };

            threads.add(new Thread(r));
        }

        // Create file/directory structure.
        for (int i = 0; i < lvlCnt; i++) {
            int lvl = i + 1;

            boolean targetToPrimary = !dual || lvl <= primaryLvlCnt;

            IgfsPath[] dirs = dirPaths.get(lvl).toArray(new IgfsPath[dirPaths.get(lvl).size()]);
            IgfsPath[] files = filePaths.get(lvl).toArray(new IgfsPath[filePaths.get(lvl).size()]);

            if (targetToPrimary)
                create(igfs, dirs, files);
            else
                create(igfsSecondary, dirs, files);
        }

        // Start all threads and wait for them to finish.
        for (Thread thread : threads)
            thread.start();

        U.joinThreads(threads, null);
    }

    /**
     * Create the given directories and files in the given IGFS.
     *
     * @param igfs IGFS.
     * @param dirs Directories.
     * @param files Files.
     * @throws Exception If failed.
     */
    public static void create(IgfsImpl igfs, @Nullable IgfsPath[] dirs, @Nullable IgfsPath[] files) throws Exception {
        create(igfs.asSecondary(), dirs, files);
    }

    /**
     * Create the given directories and files in the given IGFS.
     *
     * @param igfs IGFS.
     * @param dirs Directories.
     * @param files Files.
     * @throws Exception If failed.
     */
    public static void create(IgfsSecondaryFileSystem igfs, @Nullable IgfsPath[] dirs, @Nullable IgfsPath[] files)
        throws Exception {
        if (dirs != null) {
            for (IgfsPath dir : dirs)
                igfs.mkdirs(dir);
        }

        if (files != null) {
            for (IgfsPath file : files) {
                OutputStream os = igfs.create(file, true);

                os.close();
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
    protected static void createFile(IgfsSecondaryFileSystem igfs, IgfsPath file, boolean overwrite,
        @Nullable byte[]... chunks) throws IOException {
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
                awaitFileClose(igfsEx.asSecondary(), file);
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

            awaitFileClose(igfs.asSecondary(), file);
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

            awaitFileClose(igfs.asSecondary(), file);
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
     * Ensure that the given paths exist in the given IGFSs.
     *
     * @param igfs First IGFS.
     * @param igfsSecondary Second IGFS.
     * @param paths Paths.
     * @throws Exception If failed.
     */
    protected void checkExist(IgfsImpl igfs, IgfsSecondaryFileSystemTestAdapter igfsSecondary, IgfsPath... paths) throws Exception {
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

        if (dual)
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

        if (dual)
            clear(igfsSecondary);
    }

    /**
     * Gets the data cache instance for this IGFS instance.
     *
     * @param igfs The IGFS unstance.
     * @return The data cache.
     */
    protected static GridCacheAdapter<IgfsBlockKey, byte[]> getDataCache(IgniteFileSystem igfs) {
        String dataCacheName = igfs.configuration().getDataCacheName();

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
        String dataCacheName = igfs.configuration().getMetaCacheName();

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
        igfs.format();

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