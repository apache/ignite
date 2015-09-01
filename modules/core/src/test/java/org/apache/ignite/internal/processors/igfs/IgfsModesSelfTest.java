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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.ignite.IgniteException;
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
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.igfs.IgfsMode.DUAL_ASYNC;
import static org.apache.ignite.igfs.IgfsMode.DUAL_SYNC;
import static org.apache.ignite.igfs.IgfsMode.PRIMARY;
import static org.apache.ignite.igfs.IgfsMode.PROXY;

/**
 * IGFS modes self test.
 */
public class IgfsModesSelfTest extends IgfsCommonAbstractTest {
    /** Grid instance hosting primary IGFS. */
    private IgniteEx grid;

    /** Primary IGFS. */
    private IgfsImpl igfs;

    /** Secondary IGFS. */
    private IgfsImpl igfsSecondary;

    /** Default IGFS mode. */
    private IgfsMode mode;

    /** Modes map. */
    private Map<String, IgfsMode> pathModes;

    /** Whether to set "null" mode. */
    private boolean setNullMode;

    /** Whether to set secondary file system URI. */
    private boolean setSecondaryFs;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        mode = null;
        pathModes = null;

        setNullMode = false;
        setSecondaryFs = false;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        G.stopAll(true);
    }

    /**
     * Perform initial startup.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("NullableProblems")
    private void startUp() throws Exception {
        startUpSecondary();

        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setDataCacheName("partitioned");
        igfsCfg.setMetaCacheName("replicated");
        igfsCfg.setName("igfs");
        igfsCfg.setBlockSize(512 * 1024);

        if (setNullMode)
            igfsCfg.setDefaultMode(null);
        else if (mode != null)
            igfsCfg.setDefaultMode(mode);

        igfsCfg.setPathModes(pathModes);

        if (setSecondaryFs)
            igfsCfg.setSecondaryFileSystem(igfsSecondary.asSecondary());

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName("partitioned");
        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setNearConfiguration(null);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(128));
        cacheCfg.setBackups(0);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setName("replicated");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName("igfs-grid");

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(metaCacheCfg, cacheCfg);
        cfg.setFileSystemConfiguration(igfsCfg);

        cfg.setLocalHost("127.0.0.1");
        cfg.setConnectorConfiguration(null);

        grid = (IgniteEx)G.start(cfg);

        igfs = (IgfsImpl)grid.fileSystem("igfs");
    }

    /**
     * Startup secondary file system.
     *
     * @throws Exception If failed.
     */
    private void startUpSecondary() throws Exception {
        FileSystemConfiguration igfsCfg = new FileSystemConfiguration();

        igfsCfg.setDataCacheName("partitioned");
        igfsCfg.setMetaCacheName("replicated");
        igfsCfg.setName("igfs-secondary");
        igfsCfg.setBlockSize(512 * 1024);
        igfsCfg.setDefaultMode(PRIMARY);

        IgfsIpcEndpointConfiguration endpointCfg = new IgfsIpcEndpointConfiguration();

        endpointCfg.setType(IgfsIpcEndpointType.TCP);
        endpointCfg.setPort(11500);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName("partitioned");
        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setNearConfiguration(null);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAffinityMapper(new IgfsGroupDataBlocksKeyMapper(128));
        cacheCfg.setBackups(0);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        CacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setName("replicated");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName("igfs-grid-secondary");

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(metaCacheCfg, cacheCfg);
        cfg.setFileSystemConfiguration(igfsCfg);

        cfg.setLocalHost("127.0.0.1");
        cfg.setConnectorConfiguration(null);

        igfsSecondary = (IgfsImpl)G.start(cfg).fileSystem("igfs-secondary");
    }

    /**
     * Set IGFS modes for particular paths.
     *
     * @param modes Modes.
     */
    @SafeVarargs
    final void pathModes(IgniteBiTuple<String, IgfsMode>... modes) {
        assert modes != null;

        pathModes = new LinkedHashMap<>(modes.length, 1.0f);

        for (IgniteBiTuple<String, IgfsMode> mode : modes)
            pathModes.put(mode.getKey(), mode.getValue());
    }

    /**
     * Test predefined path modes for PRIMARY mode.
     *
     * @throws Exception If failed.
     */
    public void testDefaultFoldersPrimary() throws Exception {
        setSecondaryFs = true;

        mode = DUAL_ASYNC;

        startUp();

        checkMode("/ignite/primary", PRIMARY);
        checkMode("/ignite/primary/", PRIMARY);
        checkMode("/ignite/primary/subfolder", PRIMARY);
        checkMode("/ignite/primary/folder/file.txt", PRIMARY);
        checkMode("/ignite/primaryx", DUAL_ASYNC);
        checkMode("/ignite/primaryx/", DUAL_ASYNC);
        checkMode("/ignite/primaryx/subfolder", DUAL_ASYNC);
        checkMode("/ignite/primaryx/folder/file.txt", DUAL_ASYNC);
    }

    /**
     * Test predefined path modes for all modes except of PRIMARY mode.
     *
     * @throws Exception If failed.
     */
    public void testDefaultFoldersNonPrimary() throws Exception {
        setSecondaryFs = true;

        mode = PRIMARY;

        startUp();

        checkMode("/ignite/proxy", PROXY);
        checkMode("/ignite/proxy/", PROXY);
        checkMode("/ignite/proxy/subfolder", PROXY);
        checkMode("/ignite/proxy/folder/file.txt", PROXY);
        checkMode("/ignite/proxyx", PRIMARY);
        checkMode("/ignite/proxyx/", PRIMARY);
        checkMode("/ignite/proxyx/subfolder", PRIMARY);
        checkMode("/ignite/proxyx/folder/file.txt", PRIMARY);

        checkMode("/userdir/ignite/proxy", PRIMARY);
        checkMode("/userdir/ignite/proxy/", PRIMARY);
        checkMode("/userdir/ignite/proxy/subfolder", PRIMARY);
        checkMode("/userdir/ignite/proxy/folder/file.txt", PRIMARY);

        checkMode("/ignite/sync", DUAL_SYNC);
        checkMode("/ignite/sync/", DUAL_SYNC);
        checkMode("/ignite/sync/subfolder", DUAL_SYNC);
        checkMode("/ignite/sync/folder/file.txt", DUAL_SYNC);
        checkMode("/ignite/syncx", PRIMARY);
        checkMode("/ignite/syncx/", PRIMARY);
        checkMode("/ignite/syncx/subfolder", PRIMARY);
        checkMode("/ignite/syncx/folder/file.txt", PRIMARY);

        checkMode("/userdir/ignite/sync", PRIMARY);
        checkMode("/userdir/ignite/sync/", PRIMARY);
        checkMode("/userdir/ignite/sync/subfolder", PRIMARY);
        checkMode("/userdir/ignite/sync/folder/file.txt", PRIMARY);

        checkMode("/ignite/async", DUAL_ASYNC);
        checkMode("/ignite/async/", DUAL_ASYNC);
        checkMode("/ignite/async/subfolder", DUAL_ASYNC);
        checkMode("/ignite/async/folder/file.txt", DUAL_ASYNC);
        checkMode("/ignite/asyncx", PRIMARY);
        checkMode("/ignite/asyncx/", PRIMARY);
        checkMode("/ignite/asyncx/subfolder", PRIMARY);
        checkMode("/ignite/asyncx/folder/file.txt", PRIMARY);

        checkMode("/userdir/ignite/async", PRIMARY);
        checkMode("/userdir/ignite/async/", PRIMARY);
        checkMode("/userdir/ignite/async/subfolder", PRIMARY);
        checkMode("/userdir/ignite/async/folder/file.txt", PRIMARY);
    }

    /**
     * Ensure that in case secondary file system URI is not provided, all predefined have no special mappings. This test
     * doesn't make sense for PRIMARY mode since in case URI is not provided DUAL_* modes automatically transforms to
     * PRIMARY and for PROXY mode we will have an exception during startup.
     *
     * @throws Exception If failed.
     */
    public void testDefaultsNoSecondaryUriNonPrimary() throws Exception {
        startUp();

        checkMode("/ignite/proxy", PRIMARY);
        checkMode("/ignite/proxy/", PRIMARY);
        checkMode("/ignite/proxy/subfolder", PRIMARY);
        checkMode("/ignite/proxy/folder/file.txt", PRIMARY);

        checkMode("/ignite/sync", PRIMARY);
        checkMode("/ignite/sync/", PRIMARY);
        checkMode("/ignite/sync/subfolder", PRIMARY);
        checkMode("/ignite/sync/folder/file.txt", PRIMARY);

        checkMode("/ignite/async", PRIMARY);
        checkMode("/ignite/async/", PRIMARY);
        checkMode("/ignite/async/subfolder", PRIMARY);
        checkMode("/ignite/async/folder/file.txt", PRIMARY);
    }

    /**
     * Ensure that it is impossible to override mappings for /ignite/* folders.
     *
     * @throws Exception If failed.
     */
    public void testDefaultFoldersOverride() throws Exception {
        setSecondaryFs = true;

        mode = DUAL_ASYNC;

        pathModes(F.t("/ignite/primary", PROXY), F.t("/ignite/proxy", DUAL_SYNC),
            F.t("/ignite/sync", DUAL_ASYNC), F.t("/ignite/async", PRIMARY));

        startUp();

        checkMode("/ignite/primary", PRIMARY);
        checkMode("/ignite/proxy", PROXY);
        checkMode("/ignite/sync", DUAL_SYNC);
        checkMode("/ignite/async", DUAL_ASYNC);
    }

    /**
     * Ensure that DUAL_ASYNC mode is set by default.
     *
     * @throws Exception If failed.
     */
    public void testModeDefaultIsNotSet() throws Exception {
        setSecondaryFs = true;

        startUp();

        checkMode("/dir", DUAL_ASYNC);
    }

    /**
     * Ensure that when mode is set, it is correctly resolved.
     *
     * @throws Exception If failed.
     */
    public void testModeDefaultIsSet() throws Exception {
        mode = DUAL_SYNC;

        setSecondaryFs = true;

        startUp();

        checkMode("/dir", DUAL_SYNC);
    }

    /**
     * Ensure that Grid doesn't start in case default mode is SECONDARY and secondary FS URI is not provided.
     *
     * @throws Exception If failed.
     */
    public void testModeSecondaryNoUri() throws Exception {
        mode = PROXY;

        String errMsg = null;

        try {
            startUp();
        }
        catch (IgniteException e) {
            errMsg = e.getCause().getCause().getMessage();
        }

        assertTrue(errMsg.startsWith(
            "Grid configuration parameter invalid: secondaryFileSystem cannot be null when mode is not PRIMARY"));
    }

    /**
     * Ensure that modes are resolved correctly when path modes are set.
     *
     * @throws Exception If failed.
     */
    public void testPathMode() throws Exception {
        pathModes(F.t("/dir1", PROXY), F.t("/dir2", DUAL_SYNC),
            F.t("/dir3", PRIMARY), F.t("/dir4", PRIMARY));

        mode = DUAL_ASYNC;

        setSecondaryFs = true;

        startUp();

        checkMode("/dir", DUAL_ASYNC);
        checkMode("/dir1", PROXY);
        checkMode("/dir2", DUAL_SYNC);

        checkMode("/dir3", PRIMARY);
        checkMode("/somedir/dir3", DUAL_ASYNC);

        checkMode("/dir4", PRIMARY);
        checkMode("/dir4/subdir", PRIMARY);
        checkMode("/somedir/dir4", DUAL_ASYNC);
        checkMode("/somedir/dir4/subdir", DUAL_ASYNC);
    }

    /**
     * Ensure that path modes switch to PRIMARY in case secondary FS config is not provided.
     *
     * @throws Exception If failed.
     */
    public void testPathModeSwitchToPrimary() throws Exception {
        mode = DUAL_SYNC;

        pathModes(F.t("/dir1", PRIMARY), F.t("/dir2", DUAL_SYNC));

        startUp();

        checkMode("/dir", PRIMARY);
        checkMode("/dir1", PRIMARY);
        checkMode("/dir2", PRIMARY);
    }

    /**
     * Ensure that Grid doesn't start in case path mode is SECONDARY and secondary FS config path is not provided.
     *
     * @throws Exception If failed.
     */
    public void testPathModeSecondaryNoCfg() throws Exception {
        pathModes(F.t("dir", PROXY));

        String errMsg = null;

        try {
            startUp();
        }
        catch (IgniteException e) {
            errMsg = e.getCause().getCause().getMessage();
        }

        assertTrue(errMsg.startsWith(
            "Grid configuration parameter invalid: secondaryFileSystem cannot be null when mode is not PRIMARY"));
    }

    /**
     * Ensure that data is not propagated to the secondary IGFS in PRIMARY mode.
     *
     * @throws Exception If failed.
     */
    public void testPropagationPrimary() throws Exception {
        mode = PRIMARY;

        checkPropagation();
    }

    /**
     * Ensure that data is propagated to the secondary IGFS in DUAL_SYNC mode.
     *
     * @throws Exception If failed.
     */
    public void testPropagationDualSync() throws Exception {
        mode = DUAL_SYNC;

        checkPropagation();
    }

    /**
     * Ensure that data is propagated to the secondary IGFS in DUAL_SYNC mode.
     *
     * @throws Exception If failed.
     */
    public void testPropagationDualAsync() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-822");

        mode = DUAL_ASYNC;

        checkPropagation();
    }

    /**
     * Resolve IGFS mode for the given path and compare it with expected one.
     *
     * @param pathStr Path ot resolve.
     * @param expMode Expected mode.
     * @throws Exception If failed.
     */
    private void checkMode(String pathStr, IgfsMode expMode) throws Exception {
        assert igfs != null;

        IgfsPath path = new IgfsPath(pathStr);

        IgfsModeResolver rslvr = igfs.modeResolver();

        IgfsMode mode = rslvr.resolveMode(path);

        assertEquals(expMode, mode);
    }

    /**
     * Check propagation of various operations to secondary file system.
     *
     * @throws Exception If failed.
     */
    private void checkPropagation() throws Exception {
        byte[] testData1 = new byte[] {0, 1, 2, 3, 4, 5, 6, 7};
        byte[] testData2 = new byte[] {8, 9, 10, 11};
        byte[] testData = Arrays.copyOf(testData1, testData1.length + testData2.length);

        U.arrayCopy(testData2, 0, testData, testData1.length, testData2.length);

        setSecondaryFs = true;

        startUp();

        boolean primaryNotUsed = mode == PROXY;
        boolean secondaryUsed = mode != PRIMARY;

        IgfsPath dir = new IgfsPath("/dir");
        IgfsPath file = new IgfsPath("/dir/file");

        // Create new directory.
        igfs.mkdirs(dir);

        // Create new file.
        IgfsOutputStream os = igfs.create(file, 1024, true, null, 0, 2048, null);

        os.write(testData1);

        os.close();

        // Re-open it and append.
        os = igfs.append(file, 1024, false, null);

        os.write(testData2);

        os.close();

        // Check file content.
        IgfsInputStream is = igfs.open(file);

        assertEquals(testData.length, is.length());

        byte[] data = new byte[testData.length];

        is.read(data, 0, testData.length);

        is.close();

        assert Arrays.equals(testData, data);

        if (secondaryUsed) {
            assert igfsSecondary.exists(dir);
            assert igfsSecondary.exists(file);

            // In ASYNC mode we wait at most 2 seconds for background writer to finish.
            for (int i = 0; i < 20; i++) {
                IgfsInputStream isSecondary = null;

                try {
                    isSecondary = igfsSecondary.open(file);

                    if (isSecondary.length() == testData.length)
                        break;
                    else
                        U.sleep(100);
                }
                finally {
                    U.closeQuiet(isSecondary);
                }
            }

            IgfsInputStream isSecondary = igfsSecondary.open(file);

            assertEquals(testData.length, isSecondary.length());

            isSecondary.read(data, 0, testData.length);

            assert Arrays.equals(testData, data);
        }
        else {
            assert !igfsSecondary.exists(dir);
            assert !igfsSecondary.exists(file);
        }

        int cacheSize = grid.cachex("partitioned").size();

        if (primaryNotUsed)
            assert cacheSize == 0;
        else
            assert cacheSize != 0;

        // Now delete all.
        igfs.delete(dir, true);

        assert !igfs.exists(dir);
        assert !igfs.exists(file);

        assert !igfsSecondary.exists(dir);
        assert !igfsSecondary.exists(file);
    }
}