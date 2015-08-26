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
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.igfs.*;
import org.apache.ignite.igfs.mapreduce.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.query.*;
import org.apache.ignite.internal.util.ipc.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;
import org.jsr166.*;

import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.IgniteSystemProperties.*;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMemoryMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.igfs.IgfsMode.*;
import static org.apache.ignite.internal.IgniteNodeAttributes.*;

/**
 * Fully operational Ignite file system processor.
 */
public class IgfsProcessor extends IgfsProcessorAdapter {
    /** Null IGFS name. */
    private static final String NULL_NAME = UUID.randomUUID().toString();

    /** Converts context to IGFS. */
    private static final IgniteClosure<IgfsContext,IgniteFileSystem> CTX_TO_IGFS = new C1<IgfsContext, IgniteFileSystem>() {
        @Override public IgniteFileSystem apply(IgfsContext igfsCtx) {
            return igfsCtx.igfs();
        }
    };

    /** */
    private final ConcurrentMap<String, IgfsContext> igfsCache =
        new ConcurrentHashMap8<>();

    /**
     * @param ctx Kernal context.
     */
    public IgfsProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        if (ctx.config().isDaemon())
            return;

        FileSystemConfiguration[] cfgs = ctx.config().getFileSystemConfiguration();

        assert cfgs != null && cfgs.length > 0;

        validateLocalIgfsConfigurations(cfgs);

        // Start IGFS instances.
        for (FileSystemConfiguration cfg : cfgs) {
            IgfsContext igfsCtx = new IgfsContext(
                ctx,
                new FileSystemConfiguration(cfg),
                new IgfsMetaManager(),
                new IgfsDataManager(),
                new IgfsServerManager(),
                new IgfsFragmentizerManager());

            // Start managers first.
            for (IgfsManager mgr : igfsCtx.managers())
                mgr.start(igfsCtx);

            igfsCache.put(maskName(cfg.getName()), igfsCtx);
        }

        if (log.isDebugEnabled())
            log.debug("IGFS processor started.");

        IgniteConfiguration gridCfg = ctx.config();

        // Node doesn't have IGFS if it:
        // is daemon;
        // doesn't have configured IGFS;
        // doesn't have configured caches.
        if (gridCfg.isDaemon() || F.isEmpty(gridCfg.getFileSystemConfiguration()) ||
            F.isEmpty(gridCfg.getCacheConfiguration()))
            return;

        final Map<String, CacheConfiguration> cacheCfgs = new HashMap<>();

        F.forEach(gridCfg.getCacheConfiguration(), new CI1<CacheConfiguration>() {
            @Override public void apply(CacheConfiguration c) {
                cacheCfgs.put(c.getName(), c);
            }
        });

        Collection<IgfsAttributes> attrVals = new ArrayList<>();

        assert gridCfg.getFileSystemConfiguration() != null;

        for (FileSystemConfiguration igfsCfg : gridCfg.getFileSystemConfiguration()) {
            CacheConfiguration cacheCfg = cacheCfgs.get(igfsCfg.getDataCacheName());

            if (cacheCfg == null)
                continue; // No cache for the given IGFS configuration.

            AffinityKeyMapper affMapper = cacheCfg.getAffinityMapper();

            if (!(affMapper instanceof IgfsGroupDataBlocksKeyMapper))
                // Do not create IGFS attributes for such a node nor throw error about invalid configuration.
                // Configuration will be validated later, while starting IgfsProcessor.
                continue;

            attrVals.add(new IgfsAttributes(
                igfsCfg.getName(),
                igfsCfg.getBlockSize(),
                ((IgfsGroupDataBlocksKeyMapper)affMapper).groupSize(),
                igfsCfg.getMetaCacheName(),
                igfsCfg.getDataCacheName(),
                igfsCfg.getDefaultMode(),
                igfsCfg.getPathModes(),
                igfsCfg.isFragmentizerEnabled()));
        }

        ctx.addNodeAttribute(ATTR_IGFS, attrVals.toArray(new IgfsAttributes[attrVals.size()]));
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws IgniteCheckedException {
        if (ctx.config().isDaemon())
            return;

        if (!getBoolean(IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK)) {
            for (ClusterNode n : ctx.discovery().remoteNodes())
                checkIgfsOnRemoteNode(n);
        }

        for (IgfsContext igfsCtx : igfsCache.values())
            for (IgfsManager mgr : igfsCtx.managers())
                mgr.onKernalStart();
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        // Stop IGFS instances.
        for (IgfsContext igfsCtx : igfsCache.values()) {
            if (log.isDebugEnabled())
                log.debug("Stopping igfs: " + igfsCtx.configuration().getName());

            List<IgfsManager> mgrs = igfsCtx.managers();

            for (ListIterator<IgfsManager> it = mgrs.listIterator(mgrs.size()); it.hasPrevious();) {
                IgfsManager mgr = it.previous();

                mgr.stop(cancel);
            }

            igfsCtx.igfs().stop(cancel);
        }

        igfsCache.clear();

        if (log.isDebugEnabled())
            log.debug("IGFS processor stopped.");
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        for (IgfsContext igfsCtx : igfsCache.values()) {
            if (log.isDebugEnabled())
                log.debug("Stopping igfs: " + igfsCtx.configuration().getName());

            List<IgfsManager> mgrs = igfsCtx.managers();

            for (ListIterator<IgfsManager> it = mgrs.listIterator(mgrs.size()); it.hasPrevious();) {
                IgfsManager mgr = it.previous();

                mgr.onKernalStop(cancel);
            }
        }

        if (log.isDebugEnabled())
            log.debug("Finished executing IGFS processor onKernalStop() callback.");
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>>");
        X.println(">>> IGFS processor memory stats [grid=" + ctx.gridName() + ']');
        X.println(">>>   igfsCacheSize: " + igfsCache.size());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Collection<IgniteFileSystem> igfss() {
        return F.viewReadOnly(igfsCache.values(), CTX_TO_IGFS);
    }

    /** {@inheritDoc} */
    @Override @Nullable public IgniteFileSystem igfs(@Nullable String name) {
        IgfsContext igfsCtx = igfsCache.get(maskName(name));

        return igfsCtx == null ? null : igfsCtx.igfs();
    }

    /** {@inheritDoc} */
    @Override @Nullable public Collection<IpcServerEndpoint> endpoints(@Nullable String name) {
        IgfsContext igfsCtx = igfsCache.get(maskName(name));

        return igfsCtx == null ? Collections.<IpcServerEndpoint>emptyList() : igfsCtx.server().endpoints();
    }

    /** {@inheritDoc} */
    @Nullable @Override public ComputeJob createJob(IgfsJob job, @Nullable String igfsName, IgfsPath path,
        long start, long len, IgfsRecordResolver recRslv) {
        return new IgfsJobImpl(job, igfsName, path, start, len, recRslv);
    }

    /**
     * @param name Cache name.
     * @return Masked name accounting for {@code nulls}.
     */
    private String maskName(@Nullable String name) {
        return name == null ? NULL_NAME : name;
    }

    /**
     * Validates local IGFS configurations. Compares attributes only for IGFSes with same name.
     * @param cfgs IGFS configurations
     * @throws IgniteCheckedException If any of IGFS configurations is invalid.
     */
    private void validateLocalIgfsConfigurations(FileSystemConfiguration[] cfgs) throws IgniteCheckedException {
        Collection<String> cfgNames = new HashSet<>();

        for (FileSystemConfiguration cfg : cfgs) {
            String name = cfg.getName();

            if (cfgNames.contains(name))
                throw new IgniteCheckedException("Duplicate IGFS name found (check configuration and " +
                    "assign unique name to each): " + name);

            CacheConfiguration dataCacheCfg = config(cfg.getDataCacheName());
            CacheConfiguration metaCacheCfg = config(cfg.getMetaCacheName());

            if (dataCacheCfg == null)
                throw new IgniteCheckedException("Data cache is not configured locally for IGFS: " + cfg);

            if (GridQueryProcessor.isEnabled(dataCacheCfg))
                throw new IgniteCheckedException("IGFS data cache cannot start with enabled query indexing.");

            if (dataCacheCfg.getAtomicityMode() != TRANSACTIONAL)
                throw new IgniteCheckedException("Data cache should be transactional: " + cfg.getDataCacheName());

            if (metaCacheCfg == null)
                throw new IgniteCheckedException("Metadata cache is not configured locally for IGFS: " + cfg);

            if (GridQueryProcessor.isEnabled(metaCacheCfg))
                throw new IgniteCheckedException("IGFS metadata cache cannot start with enabled query indexing.");

            if (GridQueryProcessor.isEnabled(metaCacheCfg))
                throw new IgniteCheckedException("IGFS metadata cache cannot start with enabled query indexing.");

            if (metaCacheCfg.getAtomicityMode() != TRANSACTIONAL)
                throw new IgniteCheckedException("Meta cache should be transactional: " + cfg.getMetaCacheName());

            if (F.eq(cfg.getDataCacheName(), cfg.getMetaCacheName()))
                throw new IgniteCheckedException("Cannot use same cache as both data and meta cache: " + cfg.getName());

            if (!(dataCacheCfg.getAffinityMapper() instanceof IgfsGroupDataBlocksKeyMapper))
                throw new IgniteCheckedException("Invalid IGFS data cache configuration (key affinity mapper class should be " +
                    IgfsGroupDataBlocksKeyMapper.class.getSimpleName() + "): " + cfg);

            long maxSpaceSize = cfg.getMaxSpaceSize();

            if (maxSpaceSize > 0) {
                // Max space validation.
                long maxHeapSize = Runtime.getRuntime().maxMemory();
                long offHeapSize = dataCacheCfg.getOffHeapMaxMemory();

                if (offHeapSize < 0 && maxSpaceSize > maxHeapSize)
                    // Offheap is disabled.
                    throw new IgniteCheckedException("Maximum IGFS space size cannot be greater that size of available heap " +
                        "memory [maxHeapSize=" + maxHeapSize + ", maxIgfsSpaceSize=" + maxSpaceSize + ']');
                else if (offHeapSize > 0 && maxSpaceSize > maxHeapSize + offHeapSize)
                    // Offheap is enabled, but limited.
                    throw new IgniteCheckedException("Maximum IGFS space size cannot be greater than size of available heap " +
                        "memory and offheap storage [maxHeapSize=" + maxHeapSize + ", offHeapSize=" + offHeapSize +
                        ", maxIgfsSpaceSize=" + maxSpaceSize + ']');
            }

            if (cfg.getMaxSpaceSize() == 0 && dataCacheCfg.getMemoryMode() == OFFHEAP_VALUES)
                U.warn(log, "IGFS max space size is not specified but data cache values are stored off-heap (max " +
                    "space will be limited to 80% of max JVM heap size): " + cfg.getName());

            boolean secondary = cfg.getDefaultMode() == PROXY;

            if (cfg.getPathModes() != null) {
                for (Map.Entry<String, IgfsMode> mode : cfg.getPathModes().entrySet()) {
                    if (mode.getValue() == PROXY)
                        secondary = true;
                }
            }

            if (secondary) {
                // When working in any mode except of primary, secondary FS config must be provided.
                assertParameter(cfg.getSecondaryFileSystem() != null,
                    "secondaryFileSystem cannot be null when mode is not " + IgfsMode.PRIMARY);
            }

            cfgNames.add(name);
        }
    }

    /**
     * @param cacheName Cache name.
     * @return Configuration.
     */
    private CacheConfiguration config(String cacheName) {
        for (CacheConfiguration ccfg : ctx.config().getCacheConfiguration()) {
            if (F.eq(cacheName, ccfg.getName()))
                return ccfg;
        }

        return null;
    }

    /**
     * Check IGFS config on remote node.
     *
     * @param rmtNode Remote node.
     * @throws IgniteCheckedException If check failed.
     */
    private void checkIgfsOnRemoteNode(ClusterNode rmtNode) throws IgniteCheckedException {
        IgfsAttributes[] locAttrs = ctx.discovery().localNode().attribute(IgniteNodeAttributes.ATTR_IGFS);
        IgfsAttributes[] rmtAttrs = rmtNode.attribute(IgniteNodeAttributes.ATTR_IGFS);

        if (F.isEmpty(locAttrs) || F.isEmpty(rmtAttrs))
            return;

        assert rmtAttrs != null && locAttrs != null;

        for (IgfsAttributes rmtAttr : rmtAttrs)
            for (IgfsAttributes locAttr : locAttrs) {
                // Checking the use of different caches on the different IGFSes.
                if (!F.eq(rmtAttr.igfsName(), locAttr.igfsName())) {
                    if (F.eq(rmtAttr.metaCacheName(), locAttr.metaCacheName()))
                        throw new IgniteCheckedException("Meta cache names should be different for different IGFS instances " +
                            "configuration (fix configuration or set " +
                            "-D" + IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK + "=true system " +
                            "property) [metaCacheName=" + rmtAttr.metaCacheName() +
                            ", locNodeId=" + ctx.localNodeId() +
                            ", rmtNodeId=" + rmtNode.id() +
                            ", locIgfsName=" + locAttr.igfsName() +
                            ", rmtIgfsName=" + rmtAttr.igfsName() + ']');

                    if (F.eq(rmtAttr.dataCacheName(), locAttr.dataCacheName()))
                        throw new IgniteCheckedException("Data cache names should be different for different IGFS instances " +
                            "configuration (fix configuration or set " +
                            "-D" + IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK + "=true system " +
                            "property)[dataCacheName=" + rmtAttr.dataCacheName() +
                            ", locNodeId=" + ctx.localNodeId() +
                            ", rmtNodeId=" + rmtNode.id() +
                            ", locIgfsName=" + locAttr.igfsName() +
                            ", rmtIgfsName=" + rmtAttr.igfsName() + ']');

                    continue;
                }

                // Compare other attributes only for IGFSes with same name.
                checkSame("Data block size", "BlockSize", rmtNode.id(), rmtAttr.blockSize(),
                    locAttr.blockSize(), rmtAttr.igfsName());

                checkSame("Affinity mapper group size", "GrpSize", rmtNode.id(), rmtAttr.groupSize(),
                    locAttr.groupSize(), rmtAttr.igfsName());

                checkSame("Meta cache name", "MetaCacheName", rmtNode.id(), rmtAttr.metaCacheName(),
                    locAttr.metaCacheName(), rmtAttr.igfsName());

                checkSame("Data cache name", "DataCacheName", rmtNode.id(), rmtAttr.dataCacheName(),
                    locAttr.dataCacheName(), rmtAttr.igfsName());

                checkSame("Default mode", "DefaultMode", rmtNode.id(), rmtAttr.defaultMode(),
                    locAttr.defaultMode(), rmtAttr.igfsName());

                checkSame("Path modes", "PathModes", rmtNode.id(), rmtAttr.pathModes(),
                    locAttr.pathModes(), rmtAttr.igfsName());

                checkSame("Fragmentizer enabled", "FragmentizerEnabled", rmtNode.id(), rmtAttr.fragmentizerEnabled(),
                    locAttr.fragmentizerEnabled(), rmtAttr.igfsName());
            }
    }

    private void checkSame(String name, String propName, UUID rmtNodeId, Object rmtVal, Object locVal, String igfsName)
        throws IgniteCheckedException {
        if (!F.eq(rmtVal, locVal))
            throw new IgniteCheckedException(name + " should be the same on all nodes in grid for IGFS configuration " +
                "(fix configuration or set " +
                "-D" + IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK + "=true system " +
                "property ) [rmtNodeId=" + rmtNodeId +
                ", rmt" + propName + "=" + rmtVal +
                ", loc" + propName + "=" + locVal +
                ", ggfName=" + igfsName + ']');
    }
}
