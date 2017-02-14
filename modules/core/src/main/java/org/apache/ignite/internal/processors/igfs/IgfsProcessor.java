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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.igfs.IgfsIpcEndpointConfiguration;
import org.apache.ignite.igfs.IgfsMode;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.mapreduce.IgfsJob;
import org.apache.ignite.igfs.mapreduce.IgfsRecordResolver;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.ipc.IpcServerEndpoint;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteClosure;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.igfs.IgfsMode.PROXY;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGFS;

/**
 * Fully operational Ignite file system processor.
 */
public class IgfsProcessor extends IgfsProcessorAdapter {
    /** Null IGFS name. */
    private static final String NULL_NAME = UUID.randomUUID().toString();

    /** Min available TCP port. */
    private static final int MIN_TCP_PORT = 1;

    /** Max available TCP port. */
    private static final int MAX_TCP_PORT = 0xFFFF;

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
        IgniteConfiguration igniteCfg = ctx.config();

        if (igniteCfg.isDaemon())
            return;

        FileSystemConfiguration[] cfgs = igniteCfg.getFileSystemConfiguration();

        assert cfgs != null && cfgs.length > 0;

        validateLocalIgfsConfigurations(cfgs);

        // Start IGFS instances.
        for (FileSystemConfiguration cfg : cfgs) {
            FileSystemConfiguration cfg0 = new FileSystemConfiguration(cfg);

            boolean metaClient = true;

            CacheConfiguration[] cacheCfgs = igniteCfg.getCacheConfiguration();

            if (cacheCfgs != null) {
                for (CacheConfiguration cacheCfg : cacheCfgs) {
                    if (F.eq(cacheCfg.getName(), cfg.getMetaCacheName())) {
                        metaClient = false;

                        break;
                    }
                }
            }

            if (igniteCfg.isClientMode() != null && igniteCfg.isClientMode())
                metaClient = true;

            IgfsContext igfsCtx = new IgfsContext(
                ctx,
                cfg0,
                new IgfsMetaManager(cfg0.isRelaxedConsistency(), metaClient),
                new IgfsDataManager(),
                new IgfsServerManager(),
                new IgfsFragmentizerManager());

            // Start managers first.
            for (IgfsManager mgr : igfsCtx.managers())
                mgr.start(igfsCtx);

            igfsCache.put(maskName(cfg0.getName()), igfsCtx);
        }

        if (log.isDebugEnabled())
            log.debug("IGFS processor started.");

        // Node doesn't have IGFS if it:
        // is daemon;
        // doesn't have configured IGFS;
        // doesn't have configured caches.
        if (igniteCfg.isDaemon() || F.isEmpty(igniteCfg.getFileSystemConfiguration()) ||
            F.isEmpty(igniteCfg.getCacheConfiguration()))
            return;

        final Map<String, CacheConfiguration> cacheCfgs = new HashMap<>();

        assert igniteCfg.getCacheConfiguration() != null;

        for (CacheConfiguration ccfg : igniteCfg.getCacheConfiguration())
            cacheCfgs.put(ccfg.getName(), ccfg);

        Collection<IgfsAttributes> attrVals = new ArrayList<>();

        assert igniteCfg.getFileSystemConfiguration() != null;

        for (FileSystemConfiguration igfsCfg : igniteCfg.getFileSystemConfiguration()) {
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
                ((IgfsGroupDataBlocksKeyMapper)affMapper).getGroupSize(),
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

        Collection<String> dataCacheNames = new HashSet<>();
        Collection<String> metaCacheNames = new HashSet<>();

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

            if (dataCacheCfg.getAtomicityMode() != TRANSACTIONAL && cfg.isFragmentizerEnabled())
                throw new IgniteCheckedException("Data cache should be transactional: " + cfg.getDataCacheName() +
                    " when fragmentizer is enabled");

            if (metaCacheCfg == null)
                throw new IgniteCheckedException("Metadata cache is not configured locally for IGFS: " + cfg);

            if (GridQueryProcessor.isEnabled(metaCacheCfg))
                throw new IgniteCheckedException("IGFS metadata cache cannot start with enabled query indexing.");

            if (metaCacheCfg.getAtomicityMode() != TRANSACTIONAL)
                throw new IgniteCheckedException("Meta cache should be transactional: " + cfg.getMetaCacheName());

            if (F.eq(cfg.getDataCacheName(), cfg.getMetaCacheName()))
                throw new IgniteCheckedException("Cannot use same cache as both data and meta cache: " + cfg.getName());

            if (dataCacheNames.contains(cfg.getDataCacheName()))
                throw new IgniteCheckedException("Data cache names should be different for different IGFS instances: "
                    + cfg.getName());

            if (metaCacheNames.contains(cfg.getMetaCacheName()))
                throw new IgniteCheckedException("Meta cache names should be different for different IGFS instances: "
                    + cfg.getName());

            if (!(dataCacheCfg.getAffinityMapper() instanceof IgfsGroupDataBlocksKeyMapper))
                throw new IgniteCheckedException("Invalid IGFS data cache configuration (key affinity mapper class should be " +
                    IgfsGroupDataBlocksKeyMapper.class.getSimpleName() + "): " + cfg);

            IgfsIpcEndpointConfiguration ipcCfg = cfg.getIpcEndpointConfiguration();

            if (ipcCfg != null) {
                final int tcpPort = ipcCfg.getPort();

                if (!(tcpPort >= MIN_TCP_PORT && tcpPort <= MAX_TCP_PORT))
                    throw new IgniteCheckedException("IGFS endpoint TCP port is out of range [" + MIN_TCP_PORT +
                        ".." + MAX_TCP_PORT + "]: " + tcpPort);

                if (ipcCfg.getThreadCount() <= 0)
                    throw new IgniteCheckedException("IGFS endpoint thread count must be positive: " +
                        ipcCfg.getThreadCount());
            }

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

            dataCacheNames.add(cfg.getDataCacheName());
            metaCacheNames.add(cfg.getMetaCacheName());

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

    /**
     * Check IGFS property equality on local and remote nodes.
     *
     * @param name Property human readable name.
     * @param propName Property name/
     * @param rmtNodeId Remote node ID.
     * @param rmtVal Remote value.
     * @param locVal Local value.
     * @param igfsName IGFS name.
     *
     * @throws IgniteCheckedException If failed.
     */
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
