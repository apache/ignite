/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.ggfs.mapreduce.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.license.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.ipc.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheMemoryMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.ggfs.GridGgfsMode.*;
import static org.gridgain.grid.kernal.GridNodeAttributes.*;
import static org.gridgain.grid.kernal.processors.license.GridLicenseSubsystem.*;

/**
 * Fully operational GGFS processor.
 */
public class GridGgfsProcessor extends GridGgfsProcessorAdapter {
    /** Null GGFS name. */
    private static final String NULL_NAME = UUID.randomUUID().toString();

    /** Converts context to GGFS. */
    private static final GridClosure<GridGgfsContext,GridGgfs> CTX_TO_GGFS = new C1<GridGgfsContext, GridGgfs>() {
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

        assert cfgs != null && cfgs.length > 0;

        // Register GGFS messages.
        GridTcpCommunicationMessageFactory.registerCommon(new GridTcpCommunicationMessageProducer() {
            @Override
            public GridTcpCommunicationMessageAdapter create(byte type) {
                switch (type) {
                    case 65:
                        return new GridGgfsAckMessage();

                    case 66:
                        return new GridGgfsBlockKey();

                    case 67:
                        return new GridGgfsBlocksMessage();

                    case 68:
                        return new GridGgfsDeleteMessage();

                    case 69:
                        return new GridGgfsFileAffinityRange();

                    case 70:
                        return new GridGgfsFragmentizerRequest();

                    case 71:
                        return new GridGgfsFragmentizerResponse();

                    case 72:
                        return new GridGgfsSyncMessage();

                    default:
                        assert false : "Invalid GGFS message type.";

                        return null;
                }
            }
        }, 65, 66, 67, 68, 69,70, 71, 72);

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

        if (!Boolean.getBoolean(GridSystemProperties.GG_SKIP_CONFIGURATION_CONSISTENCY_CHECK)) {
            for (GridNode n : ctx.discovery().remoteNodes())
                checkGgfsOnRemoteNode(n);
        }

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

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Collection<GridGgfs> ggfss() {
        return F.viewReadOnly(ggfsCache.values(), CTX_TO_GGFS);
    }

    /** {@inheritDoc} */
    @Override @Nullable public GridGgfs ggfs(@Nullable String name) {
        GridGgfsContext ggfsCtx = ggfsCache.get(maskName(name));

        return ggfsCtx == null ? null : ggfsCtx.ggfs();
    }

    /** {@inheritDoc} */
    @Override @Nullable public Collection<GridIpcServerEndpoint> endpoints(@Nullable String name) {
        GridGgfsContext ggfsCtx = ggfsCache.get(maskName(name));

        return ggfsCtx == null ? Collections.<GridIpcServerEndpoint>emptyList() : ggfsCtx.server().endpoints();
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridComputeJob createJob(GridGgfsJob job, @Nullable String ggfsName, GridGgfsPath path,
        long start, long length, GridGgfsRecordResolver recRslv) {
        return new GridGgfsJobImpl(job, ggfsName, path, start, length, recRslv);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void addAttributes(Map<String, Object> attrs) throws GridException {
        super.addAttributes(attrs);

        GridConfiguration gridCfg = ctx.config();

        // Node doesn't have GGFS if it:
        // is daemon;
        // doesn't have configured GGFS;
        // doesn't have configured caches.
        if (gridCfg.isDaemon() || F.isEmpty(gridCfg.getGgfsConfiguration()) ||
            F.isEmpty(gridCfg.getCacheConfiguration()))
            return;

        final Map<String, GridCacheConfiguration> cacheCfgs = new HashMap<>();

        F.forEach(gridCfg.getCacheConfiguration(), new CI1<GridCacheConfiguration>() {
            @Override public void apply(GridCacheConfiguration c) {
                cacheCfgs.put(c.getName(), c);
            }
        });

        Collection<GridGgfsAttributes> attrVals = new ArrayList<>();

        assert gridCfg.getGgfsConfiguration() != null;

        for (GridGgfsConfiguration ggfsCfg : gridCfg.getGgfsConfiguration()) {
            GridCacheConfiguration cacheCfg = cacheCfgs.get(ggfsCfg.getDataCacheName());

            if (cacheCfg == null)
                continue; // No cache for the given GGFS configuration.

            GridCacheAffinityKeyMapper affMapper = cacheCfg.getAffinityMapper();

            if (!(affMapper instanceof GridGgfsGroupDataBlocksKeyMapper))
                // Do not create GGFS attributes for such a node nor throw error about invalid configuration.
                // Configuration will be validated later, while starting GridGgfsProcessor.
                continue;

            attrVals.add(new GridGgfsAttributes(
                ggfsCfg.getName(),
                ggfsCfg.getBlockSize(),
                ((GridGgfsGroupDataBlocksKeyMapper)affMapper).groupSize(),
                ggfsCfg.getMetaCacheName(),
                ggfsCfg.getDataCacheName(),
                ggfsCfg.getDefaultMode(),
                ggfsCfg.getPathModes(),
                ggfsCfg.isFragmentizerEnabled()));
        }

        attrs.put(ATTR_GGFS, attrVals.toArray(new GridGgfsAttributes[attrVals.size()]));
    }

    /**
     * @param name Cache name.
     * @return Masked name accounting for {@code nulls}.
     */
    private String maskName(@Nullable String name) {
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

            if (dataCache.configuration().isQueryIndexEnabled())
                throw new GridException("GGFS data cache cannot start with enabled query indexing.");

            GridCache<Object, Object> metaCache = ctx.cache().cache(cfg.getMetaCacheName());

            if (metaCache == null)
                throw new GridException("Metadata cache is not configured locally for GGFS: " + cfg);

            if (metaCache.configuration().isQueryIndexEnabled())
                throw new GridException("GGFS metadata cache cannot start with enabled query indexing.");

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
                assertParameter(cfg.getSecondaryFileSystem() != null,
                    "secondaryFileSystem cannot be null when mode is SECONDARY");
            }

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

        assert rmtAttrs != null && locAttrs != null;

        for (GridGgfsAttributes rmtAttr : rmtAttrs)
            for (GridGgfsAttributes locAttr : locAttrs) {
                // Checking the use of different caches on the different GGFSes.
                if (!F.eq(rmtAttr.ggfsName(), locAttr.ggfsName())) {
                    if (F.eq(rmtAttr.metaCacheName(), locAttr.metaCacheName()))
                        throw new GridException("Meta cache names should be different for different GGFS instances " +
                            "configuration (fix configuration or set " +
                            "-D" + GridSystemProperties.GG_SKIP_CONFIGURATION_CONSISTENCY_CHECK + "=true system " +
                            "property) [metaCacheName=" + rmtAttr.metaCacheName() +
                            ", locNodeId=" + ctx.localNodeId() +
                            ", rmtNodeId=" + rmtNode.id() +
                            ", locGgfsName=" + locAttr.ggfsName() +
                            ", rmtGgfsName=" + rmtAttr.ggfsName() + ']');

                    if (F.eq(rmtAttr.dataCacheName(), locAttr.dataCacheName()))
                        throw new GridException("Data cache names should be different for different GGFS instances " +
                            "configuration (fix configuration or set " +
                            "-D" + GridSystemProperties.GG_SKIP_CONFIGURATION_CONSISTENCY_CHECK + "=true system " +
                            "property)[dataCacheName=" + rmtAttr.dataCacheName() +
                            ", locNodeId=" + ctx.localNodeId() +
                            ", rmtNodeId=" + rmtNode.id() +
                            ", locGgfsName=" + locAttr.ggfsName() +
                            ", rmtGgfsName=" + rmtAttr.ggfsName() + ']');

                    continue;
                }

                // Compare other attributes only for GGFSes with same name.
                checkSame("Data block size", "BlockSize", rmtNode.id(), rmtAttr.blockSize(),
                    locAttr.blockSize(), rmtAttr.ggfsName());

                checkSame("Affinity mapper group size", "GrpSize", rmtNode.id(), rmtAttr.groupSize(),
                    locAttr.groupSize(), rmtAttr.ggfsName());

                checkSame("Meta cache name", "MetaCacheName", rmtNode.id(), rmtAttr.metaCacheName(),
                    locAttr.metaCacheName(), rmtAttr.ggfsName());

                checkSame("Data cache name", "DataCacheName", rmtNode.id(), rmtAttr.dataCacheName(),
                    locAttr.dataCacheName(), rmtAttr.ggfsName());

                checkSame("Default mode", "DefaultMode", rmtNode.id(), rmtAttr.defaultMode(),
                    locAttr.defaultMode(), rmtAttr.ggfsName());

                checkSame("Path modes", "PathModes", rmtNode.id(), rmtAttr.pathModes(),
                    locAttr.pathModes(), rmtAttr.ggfsName());

                checkSame("Fragmentizer enabled", "FragmentizerEnabled", rmtNode.id(), rmtAttr.fragmentizerEnabled(),
                    locAttr.fragmentizerEnabled(), rmtAttr.ggfsName());
            }
    }

    private void checkSame(String name, String propName, UUID rmtNodeId, Object rmtVal, Object locVal, String ggfsName)
        throws GridException {
        if (!F.eq(rmtVal, locVal))
            throw new GridException(name + " should be the same on all nodes in grid for GGFS configuration " +
                "(fix configuration or set " +
                "-D" + GridSystemProperties.GG_SKIP_CONFIGURATION_CONSISTENCY_CHECK + "=true system " +
                "property ) [rmtNodeId=" + rmtNodeId +
                ", rmt" + propName + "=" + rmtVal +
                ", loc" + propName + "=" + locVal +
                ", ggfName=" + ggfsName + ']');
    }
}
