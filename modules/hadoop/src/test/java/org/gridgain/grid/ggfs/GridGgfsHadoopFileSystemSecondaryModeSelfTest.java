/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.ggfs;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.ggfs.hadoop.v1.*;
import org.gridgain.grid.kernal.ggfs.hadoop.*;
import org.gridgain.grid.kernal.processors.ggfs.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;

import java.net.*;
import java.util.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.ggfs.GridGgfsMode.*;

/**
 * Ensures correct modes resolution for SECONDARY paths.
 */
public class GridGgfsHadoopFileSystemSecondaryModeSelfTest extends GridGgfsCommonAbstractTest {
    /** Path to check. */
    private static final Path PATH = new Path("/dir");

    /** Pattern matching the path. */
    private static final String PATTERN_MATCHES = "/dir";

    /** Pattern doesn't matching the path. */
    private static final String PATTERN_NOT_MATCHES = "/files";

    /** Default GGFS mode. */
    private GridGgfsMode mode;

    /** Path modes. */
    private Map<String, GridGgfsMode> pathModes;

    /** File system. */
    private GridGgfsHadoopFileSystem fs;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        mode = null;
        pathModes = null;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        U.closeQuiet(fs);

        fs = null;

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

        GridGgfsConfiguration ggfsCfg = new GridGgfsConfiguration();

        ggfsCfg.setDataCacheName("partitioned");
        ggfsCfg.setMetaCacheName("replicated");
        ggfsCfg.setName("ggfs");
        ggfsCfg.setBlockSize(512 * 1024);
        ggfsCfg.setDefaultMode(mode);
        ggfsCfg.setPathModes(pathModes);
        ggfsCfg.setIpcEndpointConfiguration(GridGgfsTestUtils.jsonToMap("{type:'tcp', port:10500}"));
        ggfsCfg.setManagementPort(-1);
        ggfsCfg.setSecondaryFileSystem(new GridGgfsHadoopFileSystemWrapper(
            "ggfs://ggfs-secondary:ggfs-grid-secondary@127.0.0.1:11500/",
            "modules/core/src/test/config/hadoop/core-site-loopback-secondary.xml"));

        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setName("partitioned");
        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setDistributionMode(GridCacheDistributionMode.PARTITIONED_ONLY);
        cacheCfg.setWriteSynchronizationMode(GridCacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setAffinityMapper(new GridGgfsGroupDataBlocksKeyMapper(128));
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

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(new GridTcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(metaCacheCfg, cacheCfg);
        cfg.setGgfsConfiguration(ggfsCfg);

        cfg.setLocalHost("127.0.0.1");

        G.start(cfg);

        Configuration fsCfg = new Configuration();

        fsCfg.addResource(U.resolveGridGainUrl("modules/core/src/test/config/hadoop/core-site-loopback.xml"));

        fsCfg.setBoolean("fs.ggfs.impl.disable.cache", true);

        fs = (GridGgfsHadoopFileSystem)FileSystem.get(new URI("ggfs://ggfs:ggfs-grid@/"), fsCfg);
    }

    /**
     * Startup secondary file system.
     *
     * @throws Exception If failed.
     */
    private void startUpSecondary() throws Exception {
        GridGgfsConfiguration ggfsCfg = new GridGgfsConfiguration();

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
        cacheCfg.setAffinityMapper(new GridGgfsGroupDataBlocksKeyMapper(128));
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

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(new GridTcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(discoSpi);
        cfg.setCacheConfiguration(metaCacheCfg, cacheCfg);
        cfg.setGgfsConfiguration(ggfsCfg);

        cfg.setLocalHost("127.0.0.1");

        G.start(cfg);
    }

    /**
     * Check path resolution when secondary mode is not default and there are no other exclusion paths.
     *
     * @throws Exception If failed.
     */
    public void testSecondaryNotDefaultNoExclusions() throws Exception {
        mode = PRIMARY;

        startUp();

        assert !secondary(PATH);
        assert !secondary(PATH);
    }

    /**
     * Check path resolution when secondary mode is not default and there is no matching exclusion paths.
     *
     * @throws Exception If failed.
     */
    public void testSecondaryNotDefaultNonMatchingExclusion() throws Exception {
        mode = PRIMARY;

        pathModes(F.t(PATTERN_NOT_MATCHES, PROXY));

        startUp();

        assert !secondary(PATH);
        assert !secondary(PATH);
    }

    /**
     * Check path resolution when secondary mode is not default and there is matching exclusion path.
     *
     * @throws Exception If failed.
     */
    public void testSecondaryNotDefaultMatchingExclusion() throws Exception {
        mode = PRIMARY;

        pathModes(F.t(PATTERN_NOT_MATCHES, PROXY), F.t(PATTERN_MATCHES, PROXY));

        startUp();

        assert secondary(PATH);
        assert secondary(PATH);
    }

    /**
     * Check path resolution when secondary mode is default and there is no exclusion paths.
     *
     * @throws Exception If failed.
     */
    public void testSecondaryDefaultNoExclusions() throws Exception {
        mode = PROXY;

        startUp();

        assert secondary(PATH);
        assert secondary(PATH);
    }

    /**
     * Check path resolution when secondary mode is default and there is no matching exclusion paths.
     *
     * @throws Exception If failed.
     */
    public void testSecondaryDefaultNonMatchingExclusion() throws Exception {
        mode = PROXY;

        pathModes(F.t(PATTERN_NOT_MATCHES, PRIMARY));

        startUp();

        assert secondary(PATH);
        assert secondary(PATH);
    }

    /**
     * Check path resolution when secondary mode is default and there is no matching exclusion paths.
     *
     * @throws Exception If failed.
     */
    public void testSecondaryDefaultMatchingExclusion() throws Exception {
        mode = PROXY;

        pathModes(F.t(PATTERN_NOT_MATCHES, PRIMARY), F.t(PATTERN_MATCHES, PRIMARY));

        startUp();

        assert !secondary(PATH);
        assert !secondary(PATH);
    }

    /**
     * Set GGFS modes for particular paths.
     *
     * @param modes Modes.
     */
    @SafeVarargs
    final void pathModes(GridBiTuple<String, GridGgfsMode>... modes) {
        assert modes != null;

        pathModes = new LinkedHashMap<>(modes.length, 1.0f);

        for (GridBiTuple<String, GridGgfsMode> mode : modes)
            pathModes.put(mode.getKey(), mode.getValue());
    }

    /**
     * Check whether the given path is threaten as SECONDARY in the file system.
     *
     * @param path Path to check.
     * @return {@code True} in case path is secondary.
     * @throws Exception If failed.
     */
    private boolean secondary(Path path) throws Exception {
        return fs.mode(path) == PROXY;
    }
}
