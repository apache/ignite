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

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathExistsException;
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.hadoop.fs.IgniteHadoopIgfsSecondaryFileSystem;
import org.apache.ignite.igfs.IgfsBlockLocation;
import org.apache.ignite.igfs.IgfsFile;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.igfs.IgfsIpcEndpointConfiguration;
import org.apache.ignite.igfs.IgfsMetrics;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.internal.processors.igfs.IgfsCommonAbstractTest;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ThreadLocalRandom8;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.events.EventType.EVT_JOB_MAPPED;
import static org.apache.ignite.events.EventType.EVT_TASK_FAILED;
import static org.apache.ignite.events.EventType.EVT_TASK_FINISHED;
import static org.apache.ignite.igfs.IgfsMode.PRIMARY;
import static org.apache.ignite.igfs.IgfsMode.PROXY;

/**
 * Hadoop 2.x compliant file system.
 */
public abstract class HadoopIgfs20FileSystemAbstractSelfTest extends IgfsCommonAbstractTest {
    /** Group size. */
    public static final int GRP_SIZE = 128;

    /** Thread count for multithreaded tests. */
    private static final int THREAD_CNT = 8;

    /** Secondary file system user. */
    private static final String SECONDARY_FS_USER = "secondary-default";

    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Barrier for multithreaded tests. */
    private static CyclicBarrier barrier;

    /** File system. */
    private static AbstractFileSystem fs;

    /** Default IGFS mode. */
    protected IgfsMode mode;

    /** Primary file system URI. */
    protected URI primaryFsUri;

    /** Primary file system configuration. */
    protected Configuration primaryFsCfg;

    /**
     * Constructor.
     *
     * @param mode Default IGFS mode.
     */
    protected HadoopIgfs20FileSystemAbstractSelfTest(IgfsMode mode) {
        this.mode = mode;
    }

    /**
     * Gets primary file system URI path.
     *
     * @return Primary file system URI path.
     */
    protected abstract String primaryFileSystemUriPath();

    /**
     * Gets primary file system config path.
     *
     * @return Primary file system config path.
     */
    protected abstract String primaryFileSystemConfigPath();

    /**
     * Get primary IPC endpoint configuration.
     *
     * @param gridName Grid name.
     * @return IPC primary endpoint configuration.
     */
    protected abstract IgfsIpcEndpointConfiguration primaryIpcEndpointConfiguration(String gridName);

    /**
     * Gets secondary file system URI path.
     *
     * @return Secondary file system URI path.
     */
    protected abstract String secondaryFileSystemUriPath();

    /**
     * Gets secondary file system config path.
     *
     * @return Secondary file system config path.
     */
    protected abstract String secondaryFileSystemConfigPath();

    /**
     * Get secondary IPC endpoint configuration.
     *
     * @return Secondary IPC endpoint configuration.
     */
    protected abstract IgfsIpcEndpointConfiguration secondaryIpcEndpointConfiguration();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startNodes();
    }

    /**
     * Starts the nodes for this test.
     *
     * @throws Exception If failed.
     */
    private void startNodes() throws Exception {
        if (mode != PRIMARY) {
            // Start secondary IGFS.
            FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

            igfsCfg.setDataCacheName("partitioned");
            igfsCfg.setMetaCacheName("replicated");
            igfsCfg.setName("igfs_secondary");
            igfsCfg.setIpcEndpointConfiguration(secondaryIpcEndpointConfiguration());
            igfsCfg.setManagementPort(-1);
            igfsCfg.setBlockSize(512 * 1024);
            igfsCfg.setPrefetchBlocks(1);

            CacheConfiguration cacheCfg = defaultCacheConfiguration();

            cacheCfg.setName("partitioned");
            cacheCfg.setCacheMode(PARTITIONED);
            cacheCfg.setNearConfiguration(null);
            cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
            cacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(GRP_SIZE));
            cacheCfg.setBackups(0);
            cacheCfg.setAtomicityMode(TRANSACTIONAL);

            CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

            metaCacheCfg.setName("replicated");
            metaCacheCfg.setCacheMode(REPLICATED);
            metaCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
            metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

            IgniteConfiguration cfg = new IgniteConfiguration();

            cfg.setGridName("grid_secondary");

            TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

            discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

            cfg.setDiscoverySpi(discoSpi);
            cfg.setCacheConfiguration(metaCacheCfg, cacheCfg);
            cfg.setFileSystemConfiguration(igfsCfg);
            cfg.setIncludeEventTypes(EVT_TASK_FAILED, EVT_TASK_FINISHED, EVT_JOB_MAPPED);
            cfg.setLocalHost(U.getLocalHost().getHostAddress());
            cfg.setCommunicationSpi(communicationSpi());

            G.start(cfg);
        }

        startGrids(4);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override public String getTestGridName() {
        return "grid";
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(cacheConfiguration(gridName));
        cfg.setFileSystemConfiguration(igfsConfiguration(gridName));
        cfg.setIncludeEventTypes(EVT_TASK_FAILED, EVT_TASK_FINISHED, EVT_JOB_MAPPED);
        cfg.setLocalHost("127.0.0.1");
        cfg.setCommunicationSpi(communicationSpi());

        return cfg;
    }

    /**
     * Gets cache configuration.
     *
     * @param gridName Grid name.
     * @return Cache configuration.
     */
    protected CacheConfiguration[] cacheConfiguration(String gridName) {
        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName("partitioned");
        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setNearConfiguration(null);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(GRP_SIZE));
        cacheCfg.setBackups(0);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setName("replicated");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        return new CacheConfiguration[] {metaCacheCfg, cacheCfg};
    }

    /**
     * Gets IGFS configuration.
     *
     * @param gridName Grid name.
     * @return IGFS configuration.
     */
    protected FileSystemConfiguration igfsConfiguration(String gridName) throws IgniteCheckedException {
        FileSystemConfiguration cfg = new FileSystemConfiguration();

        cfg.setDataCacheName("partitioned");
        cfg.setMetaCacheName("replicated");
        cfg.setName("igfs");
        cfg.setPrefetchBlocks(1);
        cfg.setMaxSpaceSize(64 * 1024 * 1024);
        cfg.setDefaultMode(mode);

        if (mode != PRIMARY)
            cfg.setSecondaryFileSystem(new IgniteHadoopIgfsSecondaryFileSystem(secondaryFileSystemUriPath(),
                secondaryFileSystemConfigPath(), SECONDARY_FS_USER));

        cfg.setIpcEndpointConfiguration(primaryIpcEndpointConfiguration(gridName));
        cfg.setManagementPort(-1);

        cfg.setBlockSize(512 * 1024); // Together with group blocks mapper will yield 64M per node groups.

        return cfg;
    }

    /** @return Communication SPI. */
    private CommunicationSpi communicationSpi() {
        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        commSpi.setSharedMemoryPort(-1);

        return commSpi;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        G.stopAll(true);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        primaryFsUri = new URI(primaryFileSystemUriPath());

        primaryFsCfg = new Configuration();

        primaryFsCfg.addResource(U.resolveIgniteUrl(primaryFileSystemConfigPath()));

        UserGroupInformation ugi = UserGroupInformation.getBestUGI(null, getClientFsUser());

        // Create Fs on behalf of the client user:
        ugi.doAs(new PrivilegedExceptionAction<Object>() {
            @Override public Object run() throws Exception {
                fs = AbstractFileSystem.get(primaryFsUri, primaryFsCfg);

                return null;
            }
        });

        barrier = new CyclicBarrier(THREAD_CNT);
    }

    /**
     * Gets the user the Fs client operates on bahalf of.
     * @return The user the Fs client operates on bahalf of.
     */
    protected String getClientFsUser() {
        return "foo";
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        try {
            HadoopIgfsUtils.clear(fs);
        }
        catch (Exception ignore) {
            // No-op.
        }

        U.closeQuiet((Closeable)fs);
    }

    /** @throws Exception If failed. */
    public void testStatus() throws Exception {
        Path file1 = new Path("/file1");

        try (FSDataOutputStream file = fs.create(file1, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault()))) {
            file.write(new byte[1024 * 1024]);
        }

        FsStatus status = fs.getFsStatus();

        assertEquals(getClientFsUser(), fs.getFileStatus(file1).getOwner());

        assertEquals(4, grid(0).cluster().nodes().size());

        long used = 0, max = 0;

        for (int i = 0; i < 4; i++) {
            IgniteFileSystem igfs = grid(i).fileSystem("igfs");

            IgfsMetrics metrics = igfs.metrics();

            used += metrics.localSpaceSize();
            max += metrics.maxSpaceSize();
        }

        assertEquals(used, status.getUsed());
        assertEquals(max, status.getCapacity());
    }

    /** @throws Exception If failed. */
    public void testTimes() throws Exception {
        Path file = new Path("/file1");

        long now = System.currentTimeMillis();

        try (FSDataOutputStream os = fs.create(file, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault()))) {
            os.write(new byte[1024 * 1024]);
        }

        FileStatus status = fs.getFileStatus(file);

        assertTrue(status.getAccessTime() >= now);
        assertTrue(status.getModificationTime() >= now);

        long accessTime = now - 10 * 60 * 1000;
        long modificationTime = now - 5 * 60 * 1000;

        fs.setTimes(file, modificationTime, accessTime);

        status = fs.getFileStatus(file);
        assertEquals(accessTime, status.getAccessTime());
        assertEquals(modificationTime, status.getModificationTime());

        // Check listing is updated as well.
        FileStatus[] files = fs.listStatus(new Path("/"));

        assertEquals(1, files.length);

        assertEquals(file.getName(), files[0].getPath().getName());
        assertEquals(accessTime, files[0].getAccessTime());
        assertEquals(modificationTime, files[0].getModificationTime());

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                fs.setTimes(new Path("/unknownFile"), 0, 0);

                return null;
            }
        }, FileNotFoundException.class, null);
    }

    /** @throws Exception If failed. */
    public void testCreateCheckParameters() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fs.create(null, EnumSet.noneOf(CreateFlag.class),
                    Options.CreateOpts.perms(FsPermission.getDefault()));
            }
        }, NullPointerException.class, null);
    }

    /** @throws Exception If failed. */
    public void testCreateBase() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path dir = new Path(fsHome, "/someDir1/someDir2/someDir3");
        Path file = new Path(dir, "someFile");

        assertPathDoesNotExist(fs, file);

        FsPermission fsPerm = new FsPermission((short)644);

        FSDataOutputStream os = fs.create(file, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(fsPerm));

        // Try to write something in file.
        os.write("abc".getBytes());

        os.close();

        // Check file status.
        FileStatus fileStatus = fs.getFileStatus(file);

        assertFalse(fileStatus.isDirectory());
        assertEquals(file, fileStatus.getPath());
        assertEquals(fsPerm, fileStatus.getPermission());
    }

    /** @throws Exception If failed. */
    public void testCreateCheckOverwrite() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path dir = new Path(fsHome, "/someDir1/someDir2/someDir3");
        final Path file = new Path(dir, "someFile");

        FSDataOutputStream out = fs.create(file, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault()));

        out.close();

        // Check intermediate directory permissions.
        assertEquals(FsPermission.getDefault(), fs.getFileStatus(dir).getPermission());
        assertEquals(FsPermission.getDefault(), fs.getFileStatus(dir.getParent()).getPermission());
        assertEquals(FsPermission.getDefault(), fs.getFileStatus(dir.getParent().getParent()).getPermission());

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fs.create(file, EnumSet.noneOf(CreateFlag.class),
                    Options.CreateOpts.perms(FsPermission.getDefault()));
            }
        }, PathExistsException.class, null);

        // Overwrite should be successful.
        FSDataOutputStream out1 = fs.create(file, EnumSet.of(CreateFlag.OVERWRITE),
            Options.CreateOpts.perms(FsPermission.getDefault()));

        out1.close();
    }

    /** @throws Exception If failed. */
    public void testDeleteIfNoSuchPath() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path dir = new Path(fsHome, "/someDir1/someDir2/someDir3");

        assertPathDoesNotExist(fs, dir);

        assertFalse(fs.delete(dir, true));
    }

    /** @throws Exception If failed. */
    public void testDeleteSuccessfulIfPathIsOpenedToRead() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        final Path file = new Path(fsHome, "myFile");

        FSDataOutputStream os = fs.create(file, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault()));

        final int cnt = 5 * FileSystemConfiguration.DFLT_BLOCK_SIZE; // Write 5 blocks.

        for (int i = 0; i < cnt; i++)
            os.writeInt(i);

        os.close();

        final FSDataInputStream is = fs.open(file, -1);

        for (int i = 0; i < cnt / 2; i++)
            assertEquals(i, is.readInt());

        assert fs.delete(file, false);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                fs.getFileStatus(file);

                return null;
            }
        }, FileNotFoundException.class, null);

        is.close();
    }

    /** @throws Exception If failed. */
    public void testDeleteIfFilePathExists() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path file = new Path(fsHome, "myFile");

        FSDataOutputStream os = fs.create(file, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault()));

        os.close();

        assertTrue(fs.delete(file, false));

        assertPathDoesNotExist(fs, file);
    }

    /** @throws Exception If failed. */
    public void testDeleteIfDirectoryPathExists() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path dir = new Path(fsHome, "/someDir1/someDir2/someDir3");

        FSDataOutputStream os = fs.create(dir, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault()));

        os.close();

        assertTrue(fs.delete(dir, false));

        assertPathDoesNotExist(fs, dir);
    }

    /** @throws Exception If failed. */
    public void testDeleteFailsIfNonRecursive() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path someDir3 = new Path(fsHome, "/someDir1/someDir2/someDir3");

        FSDataOutputStream os = fs.create(someDir3, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault()));

        os.close();

        final Path someDir2 = new Path(fsHome, "/someDir1/someDir2");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                fs.delete(someDir2, false);

                return null;
            }
        }, PathIsNotEmptyDirectoryException.class, null);

        assertPathExists(fs, someDir2);
        assertPathExists(fs, someDir3);
    }

    /** @throws Exception If failed. */
    public void testDeleteRecursively() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path someDir3 = new Path(fsHome, "/someDir1/someDir2/someDir3");

        FSDataOutputStream os = fs.create(someDir3, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault()));

        os.close();

        Path someDir2 = new Path(fsHome, "/someDir1/someDir2");

        assertTrue(fs.delete(someDir2, true));

        assertPathDoesNotExist(fs, someDir2);
        assertPathDoesNotExist(fs, someDir3);
    }

    /** @throws Exception If failed. */
    public void testDeleteRecursivelyFromRoot() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path someDir3 = new Path(fsHome, "/someDir1/someDir2/someDir3");

        FSDataOutputStream os = fs.create(someDir3, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault()));

        os.close();

        Path root = new Path(fsHome, "/");

        assertFalse(fs.delete(root, true));

        assertTrue(fs.delete(new Path(fsHome, "/someDir1"), true));

        assertPathDoesNotExist(fs, someDir3);
        assertPathDoesNotExist(fs, new Path(fsHome, "/someDir1/someDir2"));
        assertPathDoesNotExist(fs, new Path(fsHome, "/someDir1"));
        assertPathExists(fs, root);
    }

    /** @throws Exception If failed. */
    public void testSetPermissionCheckDefaultPermission() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path file = new Path(fsHome, "/tmp/my");

        FSDataOutputStream os = fs.create(file, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault()));

        os.close();

        fs.setPermission(file, null);

        assertEquals(FsPermission.getDefault(), fs.getFileStatus(file).getPermission());
        assertEquals(FsPermission.getDefault(), fs.getFileStatus(file.getParent()).getPermission());
    }

    /** @throws Exception If failed. */
    public void testSetPermissionCheckNonRecursiveness() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path file = new Path(fsHome, "/tmp/my");

        FSDataOutputStream os = fs.create(file, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault()));

        os.close();

        Path tmpDir = new Path(fsHome, "/tmp");

        FsPermission perm = new FsPermission((short)123);

        fs.setPermission(tmpDir, perm);

        assertEquals(perm, fs.getFileStatus(tmpDir).getPermission());
        assertEquals(FsPermission.getDefault(), fs.getFileStatus(file).getPermission());
    }

    /** @throws Exception If failed. */
    @SuppressWarnings("OctalInteger")
    public void testSetPermission() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path file = new Path(fsHome, "/tmp/my");

        FSDataOutputStream os = fs.create(file, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault()));

        os.close();

        for (short i = 0; i <= 0777; i += 7) {
            FsPermission perm = new FsPermission(i);

            fs.setPermission(file, perm);

            assertEquals(perm, fs.getFileStatus(file).getPermission());
        }
    }

    /** @throws Exception If failed. */
    public void testSetPermissionIfOutputStreamIsNotClosed() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path file = new Path(fsHome, "myFile");

        FsPermission perm = new FsPermission((short)123);

        FSDataOutputStream os = fs.create(file, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault()));

        fs.setPermission(file, perm);

        os.close();

        assertEquals(perm, fs.getFileStatus(file).getPermission());
    }

    /** @throws Exception If failed. */
    public void testSetOwnerCheckParametersPathIsNull() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        final Path file = new Path(fsHome, "/tmp/my");

        FSDataOutputStream os = fs.create(file, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault()));

        os.close();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                fs.setOwner(null, "aUser", "aGroup");

                return null;
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: p");
    }

    /** @throws Exception If failed. */
    public void testSetOwnerCheckParametersUserIsNull() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        final Path file = new Path(fsHome, "/tmp/my");

        FSDataOutputStream os = fs.create(file, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault()));

        os.close();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                fs.setOwner(file, null, "aGroup");

                return null;
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: username");
    }

    /** @throws Exception If failed. */
    public void testSetOwnerCheckParametersGroupIsNull() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        final Path file = new Path(fsHome, "/tmp/my");

        FSDataOutputStream os = fs.create(file, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault()));

        os.close();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                fs.setOwner(file, "aUser", null);

                return null;
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: grpName");
    }

    /** @throws Exception If failed. */
    public void testSetOwner() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        final Path file = new Path(fsHome, "/tmp/my");

        FSDataOutputStream os = fs.create(file, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault()));

        os.close();

        assertEquals(getClientFsUser(), fs.getFileStatus(file).getOwner());

        fs.setOwner(file, "aUser", "aGroup");

        assertEquals("aUser", fs.getFileStatus(file).getOwner());
        assertEquals("aGroup", fs.getFileStatus(file).getGroup());
    }

    /** @throws Exception If failed. */
    public void testSetOwnerIfOutputStreamIsNotClosed() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path file = new Path(fsHome, "myFile");

        FSDataOutputStream os = fs.create(file, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault()));

        fs.setOwner(file, "aUser", "aGroup");

        os.close();

        assertEquals("aUser", fs.getFileStatus(file).getOwner());
        assertEquals("aGroup", fs.getFileStatus(file).getGroup());
    }

    /** @throws Exception If failed. */
    public void testSetOwnerCheckNonRecursiveness() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path file = new Path(fsHome, "/tmp/my");

        FSDataOutputStream os = fs.create(file, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault()));

        os.close();

        Path tmpDir = new Path(fsHome, "/tmp");

        fs.setOwner(file, "fUser", "fGroup");
        fs.setOwner(tmpDir, "dUser", "dGroup");

        assertEquals("dUser", fs.getFileStatus(tmpDir).getOwner());
        assertEquals("dGroup", fs.getFileStatus(tmpDir).getGroup());

        assertEquals("fUser", fs.getFileStatus(file).getOwner());
        assertEquals("fGroup", fs.getFileStatus(file).getGroup());
    }

    /** @throws Exception If failed. */
    public void testOpenCheckParametersPathIsNull() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fs.open(null, 1024);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: f");
    }

    /** @throws Exception If failed. */
    public void testOpenNoSuchPath() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        final Path file = new Path(fsHome, "someFile");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fs.open(file, 1024);
            }
        }, FileNotFoundException.class, null);
    }

    /** @throws Exception If failed. */
    public void testOpenIfPathIsAlreadyOpened() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path file = new Path(fsHome, "someFile");

        FSDataOutputStream os = fs.create(file, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault()));

        os.close();

        FSDataInputStream is1 = fs.open(file);
        FSDataInputStream is2 = fs.open(file);

        is1.close();
        is2.close();
    }

    /** @throws Exception If failed. */
    public void testOpen() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path file = new Path(fsHome, "someFile");

        int cnt = 2 * 1024;

        try (FSDataOutputStream out = fs.create(file, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault()))) {

            for (long i = 0; i < cnt; i++)
                out.writeLong(i);
        }

        assertEquals(getClientFsUser(), fs.getFileStatus(file).getOwner());

        try (FSDataInputStream in = fs.open(file, 1024)) {

            for (long i = 0; i < cnt; i++)
                assertEquals(i, in.readLong());
        }
    }

    /** @throws Exception If failed. */
    public void testAppendIfPathPointsToDirectory() throws Exception {
        final Path fsHome = new Path(primaryFsUri);
        final Path dir = new Path(fsHome, "/tmp");
        Path file = new Path(dir, "my");

        FSDataOutputStream os = fs.create(file, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault()));

        os.close();

        GridTestUtils.assertThrowsInherited(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fs.create(new Path(fsHome, dir), EnumSet.of(CreateFlag.APPEND),
                    Options.CreateOpts.perms(FsPermission.getDefault()));
            }
        }, IOException.class, null);
    }

    /** @throws Exception If failed. */
    public void testAppendIfFileIsAlreadyBeingOpenedToWrite() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        final Path file = new Path(fsHome, "someFile");

        FSDataOutputStream os = fs.create(file, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault()));

        os.close();

        FSDataOutputStream appendOs = fs.create(file, EnumSet.of(CreateFlag.APPEND),
            Options.CreateOpts.perms(FsPermission.getDefault()));

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fs.create(file, EnumSet.of(CreateFlag.APPEND),
                    Options.CreateOpts.perms(FsPermission.getDefault()));
            }
        }, IOException.class, null);

        appendOs.close();
    }

    /** @throws Exception If failed. */
    public void testAppend() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path file = new Path(fsHome, "someFile");

        int cnt = 1024;

        FSDataOutputStream out = fs.create(file, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault()));

        for (int i = 0; i < cnt; i++)
            out.writeLong(i);

        out.close();

        out = fs.create(file, EnumSet.of(CreateFlag.APPEND),
            Options.CreateOpts.perms(FsPermission.getDefault()));

        for (int i = cnt; i < cnt * 2; i++)
            out.writeLong(i);

        out.close();

        FSDataInputStream in = fs.open(file, 1024);

        for (int i = 0; i < cnt * 2; i++)
            assertEquals(i, in.readLong());

        in.close();
    }

    /** @throws Exception If failed. */
    public void testRenameCheckParametersSrcPathIsNull() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        final Path file = new Path(fsHome, "someFile");

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                fs.rename(null, file);

                return null;
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: f");
    }

    /** @throws Exception If failed. */
    public void testRenameCheckParametersDstPathIsNull() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        final Path file = new Path(fsHome, "someFile");

        fs.create(file, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault())).close();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                fs.rename(file, null);

                return null;
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: f");
    }

    /** @throws Exception If failed. */
    public void testRenameIfSrcPathDoesNotExist() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        final Path srcFile = new Path(fsHome, "srcFile");
        final Path dstFile = new Path(fsHome, "dstFile");

        assertPathDoesNotExist(fs, srcFile);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                fs.rename(srcFile, dstFile);

                return null;
            }
        }, FileNotFoundException.class, null);

        assertPathDoesNotExist(fs, dstFile);
    }

    /** @throws Exception If failed. */
    public void testRenameIfSrcPathIsAlreadyBeingOpenedToWrite() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path srcFile = new Path(fsHome, "srcFile");
        Path dstFile = new Path(fsHome, "dstFile");

        FSDataOutputStream os = fs.create(srcFile, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault()));

        os.close();

        os = fs.create(srcFile, EnumSet.of(CreateFlag.APPEND),
            Options.CreateOpts.perms(FsPermission.getDefault()));

        fs.rename(srcFile, dstFile);

        assertPathExists(fs, dstFile);

        String testStr = "Test";

        try {
            os.writeBytes(testStr);
        }
        finally {
            os.close();
        }

        try (FSDataInputStream is = fs.open(dstFile)) {
            byte[] buf = new byte[testStr.getBytes().length];

            is.readFully(buf);

            assertEquals(testStr, new String(buf));
        }
    }

    /** @throws Exception If failed. */
    public void testRenameFileIfDstPathExists() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        final Path srcFile = new Path(fsHome, "srcFile");
        final Path dstFile = new Path(fsHome, "dstFile");

        FSDataOutputStream os = fs.create(srcFile, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault()));

        os.close();

        os = fs.create(dstFile, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault()));

        os.close();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                fs.rename(srcFile, dstFile);

                return null;
            }
        }, FileAlreadyExistsException.class, null);

        assertPathExists(fs, srcFile);
        assertPathExists(fs, dstFile);
    }

    /** @throws Exception If failed. */
    public void testRenameFile() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path srcFile = new Path(fsHome, "/tmp/srcFile");
        Path dstFile = new Path(fsHome, "/tmp/dstFile");

        FSDataOutputStream os = fs.create(srcFile, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault()));

        os.close();

        fs.rename(srcFile, dstFile);

        assertPathDoesNotExist(fs, srcFile);
        assertPathExists(fs, dstFile);
    }

    /** @throws Exception If failed. */
    public void testRenameIfSrcPathIsAlreadyBeingOpenedToRead() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path srcFile = new Path(fsHome, "srcFile");
        Path dstFile = new Path(fsHome, "dstFile");

        FSDataOutputStream os = fs.create(srcFile, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault()));

        int cnt = 1024;

        for (int i = 0; i < cnt; i++)
            os.writeInt(i);

        os.close();

        FSDataInputStream is = fs.open(srcFile);

        for (int i = 0; i < cnt; i++) {
            if (i == 100)
                // Rename file during the read process.
                fs.rename(srcFile, dstFile);

            assertEquals(i, is.readInt());
        }

        assertPathDoesNotExist(fs, srcFile);
        assertPathExists(fs, dstFile);

        os.close();
        is.close();
    }

    /**
     * @throws Exception If failed.
     */
    public void testRenameDirectoryIfDstPathExists() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path srcDir = new Path(fsHome, "/tmp/");
        Path dstDir = new Path(fsHome, "/tmpNew/");

        FSDataOutputStream os = fs.create(new Path(srcDir, "file1"), EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault()));

        os.close();

        os = fs.create(new Path(dstDir, "file2"), EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault()));

        os.close();

        try {
            fs.rename(srcDir, dstDir);

            fail("FileAlreadyExistsException expected.");
        }
        catch (FileAlreadyExistsException ignore) {
            // No-op.
        }

        // Check all the files stay unchanged:
        assertPathExists(fs, dstDir);
        assertPathExists(fs, new Path(dstDir, "file2"));

        assertPathExists(fs, srcDir);
        assertPathExists(fs, new Path(srcDir, "file1"));
    }

    /** @throws Exception If failed. */
    public void testRenameDirectory() throws Exception {
        Path fsHome = new Path(primaryFsUri);
        Path dir = new Path(fsHome, "/tmp/");
        Path newDir = new Path(fsHome, "/tmpNew/");

        FSDataOutputStream os = fs.create(new Path(dir, "myFile"), EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault()));

        os.close();

        fs.rename(dir, newDir);

        assertPathDoesNotExist(fs, dir);
        assertPathExists(fs, newDir);
    }

    /** @throws Exception If failed. */
    public void testListStatusIfPathIsNull() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fs.listStatus(null);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: f");
    }

    /** @throws Exception If failed. */
    public void testListStatusIfPathDoesNotExist() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fs.listStatus(new Path("/someDir"));
            }
        }, FileNotFoundException.class, null);
    }

    /**
     * Test directory listing.
     *
     * @throws Exception If failed.
     */
    public void testListStatus() throws Exception {
        Path igfsHome = new Path(primaryFsUri);

        // Test listing of an empty directory.
        Path dir = new Path(igfsHome, "dir");

        fs.mkdir(dir, FsPermission.getDefault(), true);

        FileStatus[] list = fs.listStatus(dir);

        assert list.length == 0;

        // Test listing of a not empty directory.
        Path subDir = new Path(dir, "subDir");

        fs.mkdir(subDir, FsPermission.getDefault(), true);

        Path file = new Path(dir, "file");

        FSDataOutputStream fos = fs.create(file, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault()));

        fos.close();

        list = fs.listStatus(dir);

        assert list.length == 2;

        String listRes1 = list[0].getPath().getName();
        String listRes2 = list[1].getPath().getName();

        assert "subDir".equals(listRes1) && "file".equals(listRes2) || "subDir".equals(listRes2) &&
            "file".equals(listRes1);

        // Test listing of a file.
        list = fs.listStatus(file);

        assert list.length == 1;

        assert "file".equals(list[0].getPath().getName());
    }

    /** @throws Exception If failed. */
    public void testMkdirsIfPathIsNull() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                fs.mkdir(null, FsPermission.getDefault(), true);

                return null;
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: f");
    }

    /** @throws Exception If failed. */
    public void testMkdirsIfPermissionIsNull() throws Exception {
        Path dir = new Path("/tmp");

        fs.mkdir(dir, null, true);

        assertEquals(FsPermission.getDefault(), fs.getFileStatus(dir).getPermission());
    }

    /** @throws Exception If failed. */
    @SuppressWarnings("OctalInteger")
    public void testMkdirs() throws Exception {
        Path fsHome = new Path(primaryFileSystemUriPath());
        Path dir = new Path(fsHome, "/tmp/staging");
        Path nestedDir = new Path(dir, "nested");

        FsPermission dirPerm = FsPermission.createImmutable((short)0700);
        FsPermission nestedDirPerm = FsPermission.createImmutable((short)111);

        fs.mkdir(dir, dirPerm, true);
        fs.mkdir(nestedDir, nestedDirPerm, true);

        assertEquals(dirPerm, fs.getFileStatus(dir).getPermission());
        assertEquals(nestedDirPerm, fs.getFileStatus(nestedDir).getPermission());

        assertEquals(getClientFsUser(), fs.getFileStatus(dir).getOwner());
        assertEquals(getClientFsUser(), fs.getFileStatus(nestedDir).getOwner());
    }

    /** @throws Exception If failed. */
    public void testGetFileStatusIfPathIsNull() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fs.getFileStatus(null);
            }
        }, NullPointerException.class, "Ouch! Argument cannot be null: f");
    }

    /** @throws Exception If failed. */
    public void testGetFileStatusIfPathDoesNotExist() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fs.getFileStatus(new Path("someDir"));
            }
        }, FileNotFoundException.class, "File not found: someDir");
    }

    /** @throws Exception If failed. */
    public void testGetFileBlockLocationsIfFileStatusIsNull() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                // Argument is checked by Hadoop.
                return fs.getFileBlockLocations(null, 1, 2);
            }
        }, NullPointerException.class, null);
    }

    /** @throws Exception If failed. */
    public void testGetFileBlockLocationsIfFileStatusReferenceNotExistingPath() throws Exception {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fs.getFileBlockLocations(new Path("/someFile"), 1, 2);
            }
        }, FileNotFoundException.class, null);
    }

    /** @throws Exception If failed. */
    public void testGetFileBlockLocations() throws Exception {
        Path igfsHome = new Path(primaryFsUri);

        Path file = new Path(igfsHome, "someFile");

        try (OutputStream out = new BufferedOutputStream(fs.create(file, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault())))) {
            byte[] data = new byte[128 * 1024];

            for (int i = 0; i < 100; i++)
                out.write(data);

            out.flush();
        }

        try (FSDataInputStream in = fs.open(file, 1024 * 1024)) {
            byte[] data = new byte[128 * 1024];

            int read;

            do {
                read = in.read(data);
            }
            while (read > 0);
        }

        FileStatus status = fs.getFileStatus(file);

        int grpLen = 128 * 512 * 1024;

        int grpCnt = (int)((status.getLen() + grpLen - 1) / grpLen);

        BlockLocation[] locations = fs.getFileBlockLocations(file, 0, status.getLen());

        assertEquals(grpCnt, locations.length);
    }

    /** @throws Exception If failed. */
    public void testZeroReplicationFactor() throws Exception {
        // This test doesn't make sense for any mode except of PRIMARY.
        if (mode == PRIMARY) {
            Path igfsHome = new Path(primaryFsUri);

            Path file = new Path(igfsHome, "someFile");

            try (FSDataOutputStream out = fs.create(file, EnumSet.noneOf(CreateFlag.class),
                Options.CreateOpts.perms(FsPermission.getDefault()), Options.CreateOpts.repFac((short)1))) {
                out.write(new byte[1024 * 1024]);
            }

            IgniteFileSystem igfs = grid(0).fileSystem("igfs");

            IgfsPath filePath = new IgfsPath("/someFile");

            IgfsFile fileInfo = igfs.info(filePath);

            Collection<IgfsBlockLocation> locations = igfs.affinity(filePath, 0, fileInfo.length());

            assertEquals(1, locations.size());

            IgfsBlockLocation location = F.first(locations);

            assertEquals(1, location.nodeIds().size());
        }
    }

    /**
     * Ensure that when running in multithreaded mode only one create() operation succeed.
     *
     * @throws Exception If failed.
     */
    public void testMultithreadedCreate() throws Exception {
        Path dir = new Path(new Path(primaryFsUri), "/dir");

        fs.mkdir(dir, FsPermission.getDefault(), true);

        final Path file = new Path(dir, "file");

        fs.create(file, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault())).close();

        final AtomicInteger cnt = new AtomicInteger();

        final Collection<Integer> errs = new GridConcurrentHashSet<>(THREAD_CNT, 1.0f, THREAD_CNT);

        multithreaded(new Runnable() {
            @Override public void run() {
                int idx = cnt.getAndIncrement();

                byte[] data = new byte[256];

                Arrays.fill(data, (byte)idx);

                FSDataOutputStream os = null;

                try {
                    os = fs.create(file, EnumSet.of(CreateFlag.OVERWRITE),
                        Options.CreateOpts.perms(FsPermission.getDefault()));

                    os.write(data);
                }
                catch (IOException ignore) {
                    errs.add(idx);
                }
                finally {
                    U.awaitQuiet(barrier);

                    U.closeQuiet(os);
                }
            }
        }, THREAD_CNT);

        // Only one thread could obtain write lock on the file.
        assert errs.size() == THREAD_CNT - 1 : "Invalid errors count [expected=" + (THREAD_CNT - 1) + ", actual=" +
            errs.size() + ']';

        int idx = -1;

        for (int i = 0; i < THREAD_CNT; i++) {
            if (!errs.remove(i)) {
                idx = i;

                break;
            }
        }

        byte[] expData = new byte[256];

        Arrays.fill(expData, (byte)idx);

        FSDataInputStream is = fs.open(file);

        byte[] data = new byte[256];

        is.read(data);

        is.close();

        assert Arrays.equals(expData, data);
    }

    /**
     * Ensure that when running in multithreaded mode only one append() operation succeed.
     *
     * @throws Exception If failed.
     */
    public void testMultithreadedAppend() throws Exception {
        Path dir = new Path(new Path(primaryFsUri), "/dir");

        fs.mkdir(dir, FsPermission.getDefault(), true);

        final Path file = new Path(dir, "file");

        fs.create(file, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault())).close();

        final AtomicInteger cnt = new AtomicInteger();

        final Collection<Integer> errs = new GridConcurrentHashSet<>(THREAD_CNT, 1.0f, THREAD_CNT);

        multithreaded(new Runnable() {
            @Override public void run() {
                int idx = cnt.getAndIncrement();

                byte[] data = new byte[256];

                Arrays.fill(data, (byte)idx);

                U.awaitQuiet(barrier);

                FSDataOutputStream os = null;

                try {
                    os = fs.create(file, EnumSet.of(CreateFlag.APPEND),
                        Options.CreateOpts.perms(FsPermission.getDefault()));

                    os.write(data);
                }
                catch (IOException ignore) {
                    errs.add(idx);
                }
                finally {
                    U.awaitQuiet(barrier);

                    U.closeQuiet(os);
                }
            }
        }, THREAD_CNT);

        // Only one thread could obtain write lock on the file.
        assert errs.size() == THREAD_CNT - 1;

        int idx = -1;

        for (int i = 0; i < THREAD_CNT; i++) {
            if (!errs.remove(i)) {
                idx = i;

                break;
            }
        }

        byte[] expData = new byte[256];

        Arrays.fill(expData, (byte)idx);

        FSDataInputStream is = fs.open(file);

        byte[] data = new byte[256];

        is.read(data);

        is.close();

        assert Arrays.equals(expData, data);
    }

    /**
     * Test concurrent reads within the file.
     *
     * @throws Exception If failed.
     */
    public void testMultithreadedOpen() throws Exception {
        final byte[] dataChunk = new byte[256];

        for (int i = 0; i < dataChunk.length; i++)
            dataChunk[i] = (byte)i;

        Path dir = new Path(new Path(primaryFsUri), "/dir");

        fs.mkdir(dir, FsPermission.getDefault(), true);

        final Path file = new Path(dir, "file");

        FSDataOutputStream os = fs.create(file, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault()));

        // Write 256 * 2048 = 512Kb of data.
        for (int i = 0; i < 2048; i++)
            os.write(dataChunk);

        os.close();

        final AtomicBoolean err = new AtomicBoolean();

        multithreaded(new Runnable() {
            @Override
            public void run() {
                FSDataInputStream is = null;

                try {
                    int pos = ThreadLocalRandom8.current().nextInt(2048);

                    try {
                        is = fs.open(file);
                    }
                    finally {
                        U.awaitQuiet(barrier);
                    }

                    is.seek(256 * pos);

                    byte[] buf = new byte[256];

                    for (int i = pos; i < 2048; i++) {
                        // First perform normal read.
                        int read = is.read(buf);

                        assert read == 256;

                        Arrays.equals(dataChunk, buf);
                    }

                    int res = is.read(buf);

                    assert res == -1;
                }
                catch (IOException ignore) {
                    err.set(true);
                }
                finally {
                    U.closeQuiet(is);
                }
            }
        }, THREAD_CNT);

        assert !err.get();
    }

    /**
     * Test concurrent creation of multiple directories.
     *
     * @throws Exception If failed.
     */
    public void testMultithreadedMkdirs() throws Exception {
        final Path dir = new Path(new Path("igfs:///"), "/dir");

        fs.mkdir(dir, FsPermission.getDefault(), true);

        final int depth = 3;
        final int entryCnt = 5;

        final AtomicBoolean err = new AtomicBoolean();

        multithreaded(new Runnable() {
            @Override public void run() {
                Deque<IgniteBiTuple<Integer, Path>> queue = new ArrayDeque<>();

                queue.add(F.t(0, dir));

                U.awaitQuiet(barrier);

                while (!queue.isEmpty()) {
                    IgniteBiTuple<Integer, Path> t = queue.pollFirst();

                    int curDepth = t.getKey();
                    Path curPath = t.getValue();

                    if (curDepth <= depth) {
                        int newDepth = curDepth + 1;

                        // Create directories.
                        for (int i = 0; i < entryCnt; i++) {
                            Path subDir = new Path(curPath, "dir-" + newDepth + "-" + i);

                            try {
                                fs.mkdir(subDir, FsPermission.getDefault(), true);
                            }
                            catch (IOException ignore) {
                                err.set(true);
                            }

                            queue.addLast(F.t(newDepth, subDir));
                        }
                    }
                }
            }
        }, THREAD_CNT);

        // Ensure there were no errors.
        assert !err.get();

        // Ensure correct folders structure.
        Deque<IgniteBiTuple<Integer, Path>> queue = new ArrayDeque<>();

        queue.add(F.t(0, dir));

        while (!queue.isEmpty()) {
            IgniteBiTuple<Integer, Path> t = queue.pollFirst();

            int curDepth = t.getKey();
            Path curPath = t.getValue();

            if (curDepth <= depth) {
                int newDepth = curDepth + 1;

                // Create directories.
                for (int i = 0; i < entryCnt; i++) {
                    Path subDir = new Path(curPath, "dir-" + newDepth + "-" + i);

                    assertNotNull(fs.getFileStatus(subDir));

                    queue.add(F.t(newDepth, subDir));
                }
            }
        }
    }

    /**
     * Test concurrent deletion of the same directory with advanced structure.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("TooBroadScope")
    public void testMultithreadedDelete() throws Exception {
        final Path dir = new Path(new Path(primaryFsUri), "/dir");

        fs.mkdir(dir, FsPermission.getDefault(), true);

        int depth = 3;
        int entryCnt = 5;

        Deque<IgniteBiTuple<Integer, Path>> queue = new ArrayDeque<>();

        queue.add(F.t(0, dir));

        while (!queue.isEmpty()) {
            IgniteBiTuple<Integer, Path> t = queue.pollFirst();

            int curDepth = t.getKey();
            Path curPath = t.getValue();

            if (curDepth < depth) {
                int newDepth = curDepth + 1;

                // Create directories.
                for (int i = 0; i < entryCnt; i++) {
                    Path subDir = new Path(curPath, "dir-" + newDepth + "-" + i);

                    fs.mkdir(subDir, FsPermission.getDefault(), true);

                    queue.addLast(F.t(newDepth, subDir));
                }
            }
            else {
                // Create files.
                for (int i = 0; i < entryCnt; i++) {
                    Path file = new Path(curPath, "file " + i);

                    fs.create(file, EnumSet.noneOf(CreateFlag.class),
                        Options.CreateOpts.perms(FsPermission.getDefault())).close();
                }
            }
        }

        final AtomicBoolean err = new AtomicBoolean();

        multithreaded(new Runnable() {
            @Override public void run() {
                try {
                    U.awaitQuiet(barrier);

                    fs.delete(dir, true);
                }
                catch (FileNotFoundException ignore) {
                    // No-op.
                }
                catch (IOException ignore) {
                    err.set(true);
                }
            }
        }, THREAD_CNT);

        // Ensure there were no errors.
        assert !err.get();

        // Ensure the directory was actually deleted.
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                fs.getFileStatus(dir);

                return null;
            }
        }, FileNotFoundException.class, null);
    }

    /** @throws Exception If failed. */
    public void testConsistency() throws Exception {
        // Default buffers values
        checkConsistency(-1, 1, -1, -1, 1, -1);
        checkConsistency(-1, 10, -1, -1, 10, -1);
        checkConsistency(-1, 100, -1, -1, 100, -1);
        checkConsistency(-1, 1000, -1, -1, 1000, -1);
        checkConsistency(-1, 10000, -1, -1, 10000, -1);
        checkConsistency(-1, 100000, -1, -1, 100000, -1);

        checkConsistency(65 * 1024 + 13, 100000, -1, -1, 100000, -1);

        checkConsistency(-1, 100000, 2 * 4 * 1024 + 17, -1, 100000, -1);

        checkConsistency(-1, 100000, -1, 65 * 1024 + 13, 100000, -1);

        checkConsistency(-1, 100000, -1, -1, 100000, 2 * 4 * 1024 + 17);

        checkConsistency(65 * 1024 + 13, 100000, 2 * 4 * 1024 + 13, 65 * 1024 + 149, 100000, 2 * 4 * 1024 + 157);
    }

    /**
     * Verifies that client reconnects after connection to the server has been lost.
     *
     * @throws Exception If error occurs.
     */
    public void testClientReconnect() throws Exception {
        final Path igfsHome = new Path(primaryFsUri);

        final Path filePath = new Path(igfsHome, "someFile");

        final FSDataOutputStream s = fs.create(filePath, EnumSet.noneOf(CreateFlag.class),
            Options.CreateOpts.perms(FsPermission.getDefault())); // Open stream before stopping IGFS.

        try {
            G.stopAll(true); // Stop the server.

            startNodes(); // Start server again.

            // Check that client is again operational.
            fs.mkdir(new Path("igfs:///dir1/dir2"), FsPermission.getDefault(), true);

            // However, the streams, opened before disconnect, should not be valid.
            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    s.write("test".getBytes());

                    s.flush();

                    return null;
                }
            }, IOException.class, null);

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    fs.getFileStatus(filePath);

                    return null;
                }
            }, FileNotFoundException.class, null);
        }
        finally {
            U.closeQuiet(s);
        }
    }

    /**
     * Verifies that client reconnects after connection to the server has been lost (multithreaded mode).
     *
     * @throws Exception If error occurs.
     */
    public void testClientReconnectMultithreaded() throws Exception {
        final ConcurrentLinkedQueue<FileSystem> q = new ConcurrentLinkedQueue<>();

        Configuration cfg = new Configuration();

        for (Map.Entry<String, String> entry : primaryFsCfg)
            cfg.set(entry.getKey(), entry.getValue());

        cfg.setBoolean("fs.igfs.impl.disable.cache", true);

        final int nClients = 16;

        // Initialize clients.
        for (int i = 0; i < nClients; i++)
            q.add(FileSystem.get(primaryFsUri, cfg));

        G.stopAll(true); // Stop the server.

        startNodes(); // Start server again.

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                FileSystem fs = q.poll();

                try {
                    // Check that client is again operational.
                    assertTrue(fs.mkdirs(new Path("igfs:///" + Thread.currentThread().getName())));

                    return true;
                }
                finally {
                    U.closeQuiet(fs);
                }
            }
        }, nClients, "test-client");
    }

    /**
     * Checks consistency of create --> open --> append --> open operations with different buffer sizes.
     *
     * @param createBufSize Buffer size used for file creation.
     * @param writeCntsInCreate Count of times to write in file creation.
     * @param openAfterCreateBufSize Buffer size used for file opening after creation.
     * @param appendBufSize Buffer size used for file appending.
     * @param writeCntsInAppend Count of times to write in file appending.
     * @param openAfterAppendBufSize Buffer size used for file opening after appending.
     * @throws Exception If failed.
     */
    private void checkConsistency(int createBufSize, int writeCntsInCreate, int openAfterCreateBufSize,
        int appendBufSize, int writeCntsInAppend, int openAfterAppendBufSize) throws Exception {
        final Path igfsHome = new Path(primaryFsUri);

        Path file = new Path(igfsHome, "/someDir/someInnerDir/someFile");

        if (createBufSize == -1)
            createBufSize = fs.getServerDefaults().getFileBufferSize();

        if (appendBufSize == -1)
            appendBufSize = fs.getServerDefaults().getFileBufferSize();

        FSDataOutputStream os = fs.create(file, EnumSet.of(CreateFlag.OVERWRITE),
            Options.CreateOpts.perms(FsPermission.getDefault()), Options.CreateOpts.bufferSize(createBufSize));

        for (int i = 0; i < writeCntsInCreate; i++)
            os.writeInt(i);

        os.close();

        FSDataInputStream is = fs.open(file, openAfterCreateBufSize);

        for (int i = 0; i < writeCntsInCreate; i++)
            assertEquals(i, is.readInt());

        is.close();

        os = fs.create(file, EnumSet.of(CreateFlag.APPEND),
            Options.CreateOpts.perms(FsPermission.getDefault()), Options.CreateOpts.bufferSize(appendBufSize));

        for (int i = writeCntsInCreate; i < writeCntsInCreate + writeCntsInAppend; i++)
            os.writeInt(i);

        os.close();

        is = fs.open(file, openAfterAppendBufSize);

        for (int i = 0; i < writeCntsInCreate + writeCntsInAppend; i++)
            assertEquals(i, is.readInt());

        is.close();
    }

    /**
     * Test expected failures for 'close' operation.
     *
     * @param fs File system to test.
     * @param msg Expected exception message.
     */
    public void assertCloseFails(final FileSystem fs, String msg) {
        GridTestUtils.assertThrows(log, new Callable() {
            @Override public Object call() throws Exception {
                fs.close();

                return null;
            }
        }, IOException.class, msg);
    }

    /**
     * Test expected failures for 'get content summary' operation.
     *
     * @param fs File system to test.
     * @param path Path to evaluate content summary for.
     */
    private void assertContentSummaryFails(final FileSystem fs, final Path path) {
        GridTestUtils.assertThrows(log, new Callable<ContentSummary>() {
            @Override public ContentSummary call() throws Exception {
                return fs.getContentSummary(path);
            }
        }, FileNotFoundException.class, null);
    }

    /**
     * Assert that a given path exists in a given FileSystem.
     *
     * @param fs FileSystem to check.
     * @param p Path to check.
     * @throws IOException if the path does not exist.
     */
    private void assertPathExists(AbstractFileSystem fs, Path p) throws IOException {
        FileStatus fileStatus = fs.getFileStatus(p);

        assertEquals(p, fileStatus.getPath());
        assertNotSame(0, fileStatus.getModificationTime());
    }

    /**
     * Check path does not exist in a given FileSystem.
     *
     * @param fs FileSystem to check.
     * @param path Path to check.
     */
    private void assertPathDoesNotExist(final AbstractFileSystem fs, final Path path) {
        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                return fs.getFileStatus(path);
            }
        }, FileNotFoundException.class, null);
    }

    /** Helper class to encapsulate source and destination folders. */
    @SuppressWarnings({"PublicInnerClass", "PublicField"})
    public static final class Config {
        /** Source file system. */
        public final AbstractFileSystem srcFs;

        /** Source path to work with. */
        public final Path src;

        /** Destination file system. */
        public final AbstractFileSystem destFs;

        /** Destination path to work with. */
        public final Path dest;

        /**
         * Copying task configuration.
         *
         * @param srcFs Source file system.
         * @param src Source path.
         * @param destFs Destination file system.
         * @param dest Destination path.
         */
        public Config(AbstractFileSystem srcFs, Path src, AbstractFileSystem destFs, Path dest) {
            this.srcFs = srcFs;
            this.src = src;
            this.destFs = destFs;
            this.dest = dest;
        }
    }

    /**
     * Convert path for exception message testing purposes.
     *
     * @param path Path.
     * @return Converted path.
     * @throws Exception If failed.
     */
    private Path convertPath(Path path) throws Exception {
        if (mode != PROXY)
            return path;
        else {
            URI secondaryUri = new URI(secondaryFileSystemUriPath());

            URI pathUri = path.toUri();

            return new Path(new URI(pathUri.getScheme() != null ? secondaryUri.getScheme() : null,
                pathUri.getAuthority() != null ? secondaryUri.getAuthority() : null, pathUri.getPath(), null, null));
        }
    }
}
