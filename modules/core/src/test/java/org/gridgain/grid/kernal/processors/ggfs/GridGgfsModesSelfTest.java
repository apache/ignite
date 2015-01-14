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

package org.gridgain.grid.kernal.processors.ggfs;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.fs.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;

import java.util.*;

import static org.apache.ignite.fs.IgniteFsMode.*;
import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * GGFS modes self test.
 */
public class GridGgfsModesSelfTest extends GridGgfsCommonAbstractTest {
    /** Grid instance hosting primary GGFS. */
    private GridEx grid;

    /** Primary GGFS. */
    private GridGgfsImpl ggfs;

    /** Secondary GGFS. */
    private GridGgfsImpl ggfsSecondary;

    /** Default GGFS mode. */
    private IgniteFsMode mode;

    /** Modes map. */
    private Map<String, IgniteFsMode> pathModes;

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

        IgniteFsConfiguration ggfsCfg = new IgniteFsConfiguration();

        ggfsCfg.setDataCacheName("partitioned");
        ggfsCfg.setMetaCacheName("replicated");
        ggfsCfg.setName("ggfs");
        ggfsCfg.setBlockSize(512 * 1024);

        if (setNullMode)
            ggfsCfg.setDefaultMode(null);
        else if (mode != null)
            ggfsCfg.setDefaultMode(mode);

        ggfsCfg.setPathModes(pathModes);

        if (setSecondaryFs)
            ggfsCfg.setSecondaryFileSystem(ggfsSecondary);

        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName("partitioned");
        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setDistributionMode(GridCacheDistributionMode.PARTITIONED_ONLY);
        cacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAffinityMapper(new IgniteFsGroupDataBlocksKeyMapper(128));
        cacheCfg.setBackups(0);
        cacheCfg.setQueryIndexEnabled(false);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        GridCacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setName("replicated");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setQueryIndexEnabled(false);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName("ggfs-grid");

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(metaCacheCfg, cacheCfg);
        cfg.setGgfsConfiguration(ggfsCfg);

        cfg.setLocalHost("127.0.0.1");
        cfg.setRestEnabled(false);

        grid = (GridEx)G.start(cfg);

        ggfs = (GridGgfsImpl)grid.fileSystem("ggfs");
    }

    /**
     * Startup secondary file system.
     *
     * @throws Exception If failed.
     */
    private void startUpSecondary() throws Exception {
        IgniteFsConfiguration ggfsCfg = new IgniteFsConfiguration();

        ggfsCfg.setDataCacheName("partitioned");
        ggfsCfg.setMetaCacheName("replicated");
        ggfsCfg.setName("ggfs-secondary");
        ggfsCfg.setBlockSize(512 * 1024);
        ggfsCfg.setDefaultMode(PRIMARY);
        ggfsCfg.setIpcEndpointConfiguration(GridGgfsTestUtils.jsonToMap("{type:'tcp', port:11500}"));

        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName("partitioned");
        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setDistributionMode(GridCacheDistributionMode.PARTITIONED_ONLY);
        cacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAffinityMapper(new IgniteFsGroupDataBlocksKeyMapper(128));
        cacheCfg.setBackups(0);
        cacheCfg.setQueryIndexEnabled(false);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        GridCacheConfiguration metaCacheCfg = defaultCacheConfiguration();

        metaCacheCfg.setName("replicated");
        metaCacheCfg.setCacheMode(REPLICATED);
        metaCacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        metaCacheCfg.setQueryIndexEnabled(false);
        metaCacheCfg.setAtomicityMode(TRANSACTIONAL);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setGridName("ggfs-grid-secondary");

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(metaCacheCfg, cacheCfg);
        cfg.setGgfsConfiguration(ggfsCfg);

        cfg.setLocalHost("127.0.0.1");
        cfg.setRestEnabled(false);

        ggfsSecondary = (GridGgfsImpl)G.start(cfg).fileSystem("ggfs-secondary");
    }

    /**
     * Set GGFS modes for particular paths.
     *
     * @param modes Modes.
     */
    @SafeVarargs
    final void pathModes(IgniteBiTuple<String, IgniteFsMode>... modes) {
        assert modes != null;

        pathModes = new LinkedHashMap<>(modes.length, 1.0f);

        for (IgniteBiTuple<String, IgniteFsMode> mode : modes)
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

        checkMode("/gridgain/primary", PRIMARY);
        checkMode("/gridgain/primary/", PRIMARY);
        checkMode("/gridgain/primary/subfolder", PRIMARY);
        checkMode("/gridgain/primary/folder/file.txt", PRIMARY);
        checkMode("/gridgain/primaryx", DUAL_ASYNC);
        checkMode("/gridgain/primaryx/", DUAL_ASYNC);
        checkMode("/gridgain/primaryx/subfolder", DUAL_ASYNC);
        checkMode("/gridgain/primaryx/folder/file.txt", DUAL_ASYNC);
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

        checkMode("/gridgain/proxy", PROXY);
        checkMode("/gridgain/proxy/", PROXY);
        checkMode("/gridgain/proxy/subfolder", PROXY);
        checkMode("/gridgain/proxy/folder/file.txt", PROXY);
        checkMode("/gridgain/proxyx", PRIMARY);
        checkMode("/gridgain/proxyx/", PRIMARY);
        checkMode("/gridgain/proxyx/subfolder", PRIMARY);
        checkMode("/gridgain/proxyx/folder/file.txt", PRIMARY);

        checkMode("/userdir/gridgain/proxy", PRIMARY);
        checkMode("/userdir/gridgain/proxy/", PRIMARY);
        checkMode("/userdir/gridgain/proxy/subfolder", PRIMARY);
        checkMode("/userdir/gridgain/proxy/folder/file.txt", PRIMARY);

        checkMode("/gridgain/sync", DUAL_SYNC);
        checkMode("/gridgain/sync/", DUAL_SYNC);
        checkMode("/gridgain/sync/subfolder", DUAL_SYNC);
        checkMode("/gridgain/sync/folder/file.txt", DUAL_SYNC);
        checkMode("/gridgain/syncx", PRIMARY);
        checkMode("/gridgain/syncx/", PRIMARY);
        checkMode("/gridgain/syncx/subfolder", PRIMARY);
        checkMode("/gridgain/syncx/folder/file.txt", PRIMARY);

        checkMode("/userdir/gridgain/sync", PRIMARY);
        checkMode("/userdir/gridgain/sync/", PRIMARY);
        checkMode("/userdir/gridgain/sync/subfolder", PRIMARY);
        checkMode("/userdir/gridgain/sync/folder/file.txt", PRIMARY);

        checkMode("/gridgain/async", DUAL_ASYNC);
        checkMode("/gridgain/async/", DUAL_ASYNC);
        checkMode("/gridgain/async/subfolder", DUAL_ASYNC);
        checkMode("/gridgain/async/folder/file.txt", DUAL_ASYNC);
        checkMode("/gridgain/asyncx", PRIMARY);
        checkMode("/gridgain/asyncx/", PRIMARY);
        checkMode("/gridgain/asyncx/subfolder", PRIMARY);
        checkMode("/gridgain/asyncx/folder/file.txt", PRIMARY);

        checkMode("/userdir/gridgain/async", PRIMARY);
        checkMode("/userdir/gridgain/async/", PRIMARY);
        checkMode("/userdir/gridgain/async/subfolder", PRIMARY);
        checkMode("/userdir/gridgain/async/folder/file.txt", PRIMARY);
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

        checkMode("/gridgain/proxy", PRIMARY);
        checkMode("/gridgain/proxy/", PRIMARY);
        checkMode("/gridgain/proxy/subfolder", PRIMARY);
        checkMode("/gridgain/proxy/folder/file.txt", PRIMARY);

        checkMode("/gridgain/sync", PRIMARY);
        checkMode("/gridgain/sync/", PRIMARY);
        checkMode("/gridgain/sync/subfolder", PRIMARY);
        checkMode("/gridgain/sync/folder/file.txt", PRIMARY);

        checkMode("/gridgain/async", PRIMARY);
        checkMode("/gridgain/async/", PRIMARY);
        checkMode("/gridgain/async/subfolder", PRIMARY);
        checkMode("/gridgain/async/folder/file.txt", PRIMARY);
    }

    /**
     * Ensure that it is impossible to override mappings for /gridgain/* folders.
     *
     * @throws Exception If failed.
     */
    public void testDefaultFoldersOverride() throws Exception {
        setSecondaryFs = true;

        mode = DUAL_ASYNC;

        pathModes(F.t("/gridgain/primary", PROXY), F.t("/gridgain/proxy", DUAL_SYNC),
            F.t("/gridgain/sync", DUAL_ASYNC), F.t("/gridgain/async", PRIMARY));

        startUp();

        checkMode("/gridgain/primary", PRIMARY);
        checkMode("/gridgain/proxy", PROXY);
        checkMode("/gridgain/sync", DUAL_SYNC);
        checkMode("/gridgain/async", DUAL_ASYNC);
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
        catch (IgniteCheckedException e) {
            errMsg = e.getCause().getMessage();
        }

        assertTrue(errMsg.startsWith(
            "Grid configuration parameter invalid: secondaryFileSystem cannot be null when mode is SECONDARY"));
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
        catch (IgniteCheckedException e) {
            errMsg = e.getCause().getMessage();
        }

        assertTrue(errMsg.startsWith(
            "Grid configuration parameter invalid: secondaryFileSystem cannot be null when mode is SECONDARY"));
    }

    /**
     * Ensure that data is not propagated to the secondary GGFS in PRIMARY mode.
     *
     * @throws Exception If failed.
     */
    public void testPropagationPrimary() throws Exception {
        mode = PRIMARY;

        checkPropagation();
    }

    /**
     * Ensure that data is propagated to the secondary GGFS in DUAL_SYNC mode.
     *
     * @throws Exception If failed.
     */
    public void testPropagationDualSync() throws Exception {
        mode = DUAL_SYNC;

        checkPropagation();
    }

    /**
     * Ensure that data is propagated to the secondary GGFS in DUAL_SYNC mode.
     *
     * @throws Exception If failed.
     */
    public void _testPropagationDualAsync() throws Exception {
        mode = DUAL_ASYNC;

        checkPropagation();
    }

    /**
     * Resolve GGFS mode for the given path and compare it with expected one.
     *
     * @param pathStr Path ot resolve.
     * @param expMode Expected mode.
     * @throws Exception If failed.
     */
    private void checkMode(String pathStr, IgniteFsMode expMode) throws Exception {
        assert ggfs != null;

        IgniteFsPath path = new IgniteFsPath(pathStr);

        GridGgfsModeResolver rslvr = ggfs.modeResolver();

        IgniteFsMode mode = rslvr.resolveMode(path);

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

        IgniteFsPath dir = new IgniteFsPath("/dir");
        IgniteFsPath file = new IgniteFsPath("/dir/file");

        // Create new directory.
        ggfs.mkdirs(dir);

        // Create new file.
        IgniteFsOutputStream os = ggfs.create(file, 1024, true, null, 0, 2048, null);

        os.write(testData1);

        os.close();

        // Re-open it and append.
        os = ggfs.append(file, 1024, false, null);

        os.write(testData2);

        os.close();

        // Check file content.
        IgniteFsInputStream is = ggfs.open(file);

        assertEquals(testData.length, is.length());

        byte[] data = new byte[testData.length];

        is.read(data, 0, testData.length);

        is.close();

        assert Arrays.equals(testData, data);

        if (secondaryUsed) {
            assert ggfsSecondary.exists(dir);
            assert ggfsSecondary.exists(file);

            // In ASYNC mode we wait at most 2 seconds for background writer to finish.
            for (int i = 0; i < 20; i++) {
                IgniteFsInputStream isSecondary = null;

                try {
                    isSecondary = ggfsSecondary.open(file);

                    if (isSecondary.length() == testData.length)
                        break;
                    else
                        U.sleep(100);
                }
                finally {
                    U.closeQuiet(isSecondary);
                }
            }

            IgniteFsInputStream isSecondary = ggfsSecondary.open(file);

            assertEquals(testData.length, isSecondary.length());

            isSecondary.read(data, 0, testData.length);

            assert Arrays.equals(testData, data);
        }
        else {
            assert !ggfsSecondary.exists(dir);
            assert !ggfsSecondary.exists(file);
        }

        int cacheSize = grid.cachex("partitioned").size();

        if (primaryNotUsed)
            assert cacheSize == 0;
        else
            assert cacheSize != 0;

        // Now delete all.
        ggfs.delete(dir, true);

        assert !ggfs.exists(dir);
        assert !ggfs.exists(file);

        assert !ggfsSecondary.exists(dir);
        assert !ggfsSecondary.exists(file);
    }
}
