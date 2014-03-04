/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.license.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.product.GridProductEdition.*;
import static org.gridgain.grid.cache.GridCacheMemoryMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.ggfs.GridGgfsMode.*;

/**
 * Responsible for high performance file system.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridGgfsProcessor extends GridProcessorAdapter {
    /** Null GGFS name. */
    private static final String NULL_NAME = UUID.randomUUID().toString();

    /** Converts context to GGFS. */
    private static final C1<GridGgfsContext, GridGgfs> CTX_TO_GGFS = new C1<GridGgfsContext, GridGgfs>() {
        @Override public GridGgfs apply(GridGgfsContext ggfsCtx) {
            return ggfsCtx.ggfs();
        }
    };

    /** */
    private final ConcurrentMap<String, GridGgfsContext> ggfsCache =
        new ConcurrentHashMap8<>();

    /**
     * @param ctx Kernal context.
     */
    public GridGgfsProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws GridException {
        if (ctx.config().isDaemon())
            return;

        GridGgfsConfiguration[] cfgs = ctx.config().getGgfsConfiguration();

        if (F.isEmpty(cfgs)) {
            if (log.isDebugEnabled())
                log.debug("Ignored GGFS processor start callback (no GGFS are configured)");

            return;
        }

        // Register HDFS edition usage with license manager.
        GridLicenseUseRegistry.onUsage(HADOOP, getClass());

        validateLocalGgfsConfigurations(cfgs);

        // Start GGFS instances.
        for (GridGgfsConfiguration cfg : cfgs) {
            GridGgfsContext ggfsCtx = new GridGgfsContext(
                ctx,
                new GridGgfsConfiguration(cfg),
                new GridGgfsMetaManager(),
                new GridGgfsDataManager(),
                new GridGgfsServerManager(),
                new GridGgfsFragmentizerManager());

            // Start managers first.
            for (GridGgfsManager mgr : ggfsCtx.managers())
                mgr.start(ggfsCtx);

            ggfsCache.put(maskName(cfg.getName()), ggfsCtx);
        }

        if (log.isDebugEnabled())
            log.debug("GGFS processor started.");
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws GridException {
        if (ctx.config().isDaemon())
            return;

        for (GridNode n : ctx.discovery().remoteNodes())
            checkGgfsOnRemoteNode(n);

        for (GridGgfsContext ggfsCtx : ggfsCache.values())
            for (GridGgfsManager mgr : ggfsCtx.managers())
                mgr.onKernalStart();
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        // Stop GGFS instances.
        for (GridGgfsContext ggfsCtx : ggfsCache.values()) {
            if (log.isDebugEnabled())
                log.debug("Stopping ggfs: " + ggfsCtx.configuration().getName());

            List<GridGgfsManager> mgrs = ggfsCtx.managers();

            for (ListIterator<GridGgfsManager> it = mgrs.listIterator(mgrs.size()); it.hasPrevious();) {
                GridGgfsManager mgr = it.previous();

                mgr.stop(cancel);
            }

            ggfsCtx.ggfs().stop();
        }

        ggfsCache.clear();

        if (log.isDebugEnabled())
            log.debug("GGFS processor stopped.");
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        for (GridGgfsContext ggfsCtx : ggfsCache.values()) {
            if (log.isDebugEnabled())
                log.debug("Stopping ggfs: " + ggfsCtx.configuration().getName());

            List<GridGgfsManager> mgrs = ggfsCtx.managers();

            for (ListIterator<GridGgfsManager> it = mgrs.listIterator(mgrs.size()); it.hasPrevious();) {
                GridGgfsManager mgr = it.previous();

                mgr.onKernalStop(cancel);
            }
        }

        if (log.isDebugEnabled())
            log.debug("Finished executing GGFS processor onKernalStop() callback.");
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>>");
        X.println(">>> GGFS processor memory stats [grid=" + ctx.gridName() + ']');
        X.println(">>>   ggfsCacheSize: " + ggfsCache.size());
    }

    /**
     * Gets GGFS instance.
     *
     * @param name (Nullable) GGFS name.
     * @return GGFS instance.
     */
    @Nullable public GridGgfs ggfs(@Nullable String name) {
        GridGgfsContext ggfsCtx = ggfsCache.get(maskName(name));

        return ggfsCtx == null ? null : ggfsCtx.ggfs();
    }

    /**
     * Gets all GGFS instances.
     *
     * @return Collection of GGFS instances.
     */
    public Collection<GridGgfs> ggfss() {
        return F.viewReadOnly(ggfsCache.values(), CTX_TO_GGFS);
    }

    /**
     * Gets all contexts.
     *
     * @return Contexts.
     */
    public Collection<GridGgfsContext> ggfsContexts() {
        return Collections.unmodifiableCollection(ggfsCache.values());
    }

    /**
     * @param name Cache name.
     * @return Masked name accounting for {@code nulls}.
     */
    private String maskName(String name) {
        return name == null ? NULL_NAME : name;
    }

    /**
     * Validates local GGFS configurations. Compares attributes only for GGFSes with same name.
     * @param cfgs GGFS configurations
     * @throws GridException If any of GGFS configurations is invalid.
     */
    private void validateLocalGgfsConfigurations(GridGgfsConfiguration[] cfgs) throws GridException {
        Collection<String> cfgNames = new HashSet<>();

        for (GridGgfsConfiguration cfg : cfgs) {
            String name = cfg.getName();

            if (cfgNames.contains(name))
                throw new GridException("Duplicate GGFS name found (check configuration and " +
                    "assign unique name to each): " + name);

            GridCacheAdapter<Object, Object> dataCache = ctx.cache().internalCache(cfg.getDataCacheName());

            if (dataCache == null)
                throw new GridException("Data cache is not configured locally for GGFS: " + cfg);

            if (ctx.cache().cache(cfg.getMetaCacheName()) == null)
                throw new GridException("Metadata cache is not configured locally for GGFS: " + cfg);

            if (F.eq(cfg.getDataCacheName(), cfg.getMetaCacheName()))
                throw new GridException("Cannot use same cache as both data and meta cache: " + cfg.getName());

            if (!(dataCache.configuration().getAffinityMapper() instanceof GridGgfsGroupDataBlocksKeyMapper))
                throw new GridException("Invalid GGFS data cache configuration (key affinity mapper class should be " +
                    GridGgfsGroupDataBlocksKeyMapper.class.getSimpleName() + "): " + cfg);

            long maxSpaceSize = cfg.getMaxSpaceSize();

            if (maxSpaceSize > 0) {
                // Max space validation.
                long maxHeapSize = Runtime.getRuntime().maxMemory();
                long offHeapSize = dataCache.configuration().getOffHeapMaxMemory();

                if (offHeapSize < 0 && maxSpaceSize > maxHeapSize)
                    // Offheap is disabled.
                    throw new GridException("Maximum GGFS space size cannot be greater that size of available heap " +
                        "memory [maxHeapSize=" + maxHeapSize + ", maxGgfsSpaceSize=" + maxSpaceSize + ']');
                else if (offHeapSize > 0 && maxSpaceSize > maxHeapSize + offHeapSize)
                    // Offheap is enabled, but limited.
                    throw new GridException("Maximum GGFS space size cannot be greater than size of available heap " +
                        "memory and offheap storage [maxHeapSize=" + maxHeapSize + ", offHeapSize=" + offHeapSize +
                        ", maxGgfsSpaceSize=" + maxSpaceSize + ']');
            }

            if (dataCache.configuration().getCacheMode() == PARTITIONED) {
                int backups = dataCache.configuration().getBackups();

                if (backups != 0)
                    throw new GridException("GGFS data cache cannot be used with backups (set backup count " +
                        "to 0 and restart the grid): " + cfg.getDataCacheName());
            }

            if (cfg.getMaxSpaceSize() == 0 && dataCache.configuration().getMemoryMode() == OFFHEAP_VALUES)
                U.warn(log, "GGFS max space size is not specified but data cache values are stored off-heap (max " +
                    "space will be limited to 80% of max JVM heap size): " + cfg.getName());

            boolean secondary = cfg.getDefaultMode() == PROXY;

            if (cfg.getPathModes() != null) {
                for (Map.Entry<String, GridGgfsMode> mode : cfg.getPathModes().entrySet()) {
                    if (mode.getValue() == PROXY)
                        secondary = true;
                }
            }

            if (secondary) {
                // When working in any mode except of primary, secondary FS config must be provided.
                assertParameter(cfg.getSecondaryHadoopFileSystemUri() != null,
                    "secondaryHadoopFileSystemUri cannot be null when mode is SECONDARY");

                assertParameter(cfg.getSecondaryHadoopFileSystemConfigPath() != null,
                    "secondaryHadoopFileSystemConfigPath cannot be null when mode is SECONDARY");
            }

            if (cfg.getSecondaryHadoopFileSystemConfigPath() != null && cfg.getSecondaryHadoopFileSystemUri() == null)
                throw new GridException("secondaryHadoopFileSystemUri cannot be null when " +
                    "secondaryHadoopFileSystemConfigPath is set.");
            if (cfg.getSecondaryHadoopFileSystemConfigPath() == null && cfg.getSecondaryHadoopFileSystemUri() != null)
                throw new GridException("secondaryHadoopFileSystemConfigPath cannot be null when " +
                    "secondaryHadoopFileSystemUri is set.");

            cfgNames.add(name);
        }
    }

    /**
     * Check GGFS config on remote node.
     *
     * @param rmtNode Remote node.
     * @throws GridException If check failed.
     */
    private void checkGgfsOnRemoteNode(GridNode rmtNode) throws GridException {
        GridGgfsAttributes[] locAttrs = ctx.discovery().localNode().attribute(GridNodeAttributes.ATTR_GGFS);
        GridGgfsAttributes[] rmtAttrs = rmtNode.attribute(GridNodeAttributes.ATTR_GGFS);

        if (F.isEmpty(locAttrs) || F.isEmpty(rmtAttrs))
            return;

        for (GridGgfsAttributes rmtAttr : rmtAttrs)
            for (GridGgfsAttributes locAttr : locAttrs) {
                // Compare attributes only for GGFSes with same name.
                if (!F.eq(rmtAttr.ggfsName(), locAttr.ggfsName()))
                    continue;

                if (!F.eq(rmtAttr.blockSize(), locAttr.blockSize()))
                    throw new GridException("Data block size should be same on all nodes in grid " +
                        "for GGFS configuration [rmtNodeId=" + rmtNode.id() +
                        ", rmtBlockSize=" + rmtAttr.blockSize() +
                        ", locBlockSize=" + locAttr.blockSize() +
                        ", ggfsName=" + rmtAttr.ggfsName() + ']');

                if (!F.eq(rmtAttr.groupSize(), locAttr.groupSize()))
                    throw new GridException("Affinity mapper group size should be the same on all nodes in " +
                        "grid for GGFS configuration [rmtNodeId=" + rmtNode.id() +
                        ", rmtGrpSize=" + rmtAttr.groupSize() +
                        ", locGrpSize=" + locAttr.groupSize() +
                        ", ggfsName=" + rmtAttr.ggfsName() + ']');

                if (!F.eq(rmtAttr.metaCacheName(), locAttr.metaCacheName()))
                    throw new GridException("Meta cache name should be the same on all nodes in grid for GGFS " +
                        "configuration [rmtNodeId=" + rmtNode.id() +
                        ", rmtMetaCacheName=" + rmtAttr.metaCacheName() +
                        ", locMetaCacheName=" + locAttr.metaCacheName() +
                        ", ggfsName=" + rmtAttr.ggfsName() + ']');

                if (!F.eq(rmtAttr.dataCacheName(), locAttr.dataCacheName()))
                    throw new GridException("Data cache name should be the same on all nodes in grid for GGFS " +
                        "configuration [rmtNodeId=" + rmtNode.id() +
                        ", rmtDataCacheName=" + rmtAttr.dataCacheName() +
                        ", locDataCacheName=" + locAttr.dataCacheName() +
                        ", ggfsName=" + rmtAttr.ggfsName() + ']');

                if (!F.eq(rmtAttr.defaultMode(), locAttr.defaultMode()))
                    throw new GridException("Default mode should be the same on all nodes in grid for GGFS " +
                        "configuration [rmtNodeId=" + rmtNode.id() +
                        ", rmtDefaultMode=" + rmtAttr.defaultMode() +
                        ", locDefaultMode=" + locAttr.defaultMode() +
                        ", ggfsName=" + rmtAttr.ggfsName() + ']');

                if (!F.eq(rmtAttr.pathModes(), locAttr.pathModes()))
                    throw new GridException("Path modes should be the same on all nodes in grid for GGFS " +
                        "configuration [rmtNodeId=" + rmtNode.id() +
                        ", rmtPathModes=" + rmtAttr.pathModes() +
                        ", locPathModes=" + locAttr.pathModes() +
                        ", ggfsName=" + rmtAttr.ggfsName() + ']');

                if (!F.eq(rmtAttr.fragmentizerEnabled(), locAttr.fragmentizerEnabled()))
                    throw new GridException("Fragmentizer should be either enabled or disabled on " +
                        "all nodes in grid for GGFS configuration [rmtNodeId=" + rmtNode.id() +
                        ", rmtFragmentizerEnabled=" + rmtAttr.fragmentizerEnabled() +
                        ", locFragmentizerEnabled=" + locAttr.fragmentizerEnabled() +
                        ", ggfsName=" + rmtAttr.ggfsName() + ']');
            }
    }
}
