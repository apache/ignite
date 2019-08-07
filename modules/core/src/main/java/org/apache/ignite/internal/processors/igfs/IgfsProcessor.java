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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.mapreduce.IgfsJob;
import org.apache.ignite.igfs.mapreduce.IgfsRecordResolver;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.util.ipc.IpcServerEndpoint;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteClosure;
import org.jetbrains.annotations.Nullable;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SKIP_CONFIGURATION_CONSISTENCY_CHECK;
import static org.apache.ignite.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGFS;

/**
 * Fully operational Ignite file system processor.
 */
public class IgfsProcessor extends IgfsProcessorAdapter {
    /** Converts context to IGFS. */
    private static final IgniteClosure<IgfsContext,IgniteFileSystem> CTX_TO_IGFS = new C1<IgfsContext, IgniteFileSystem>() {
        @Override public IgniteFileSystem apply(IgfsContext igfsCtx) {
            return igfsCtx.igfs();
        }
    };

    /** */
    private final ConcurrentMap<String, IgfsContext> igfsCache =
        new ConcurrentHashMap<>();

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

        // Start IGFS instances.
        for (FileSystemConfiguration cfg : cfgs) {
            assert cfg.getName() != null;

            FileSystemConfiguration cfg0 = new FileSystemConfiguration(cfg);

            boolean metaClient = true;

            CacheConfiguration[] cacheCfgs = igniteCfg.getCacheConfiguration();

            String metaCacheName = cfg.getMetaCacheConfiguration().getName();

            if (cacheCfgs != null) {
                for (CacheConfiguration cacheCfg : cacheCfgs) {
                    if (F.eq(cacheCfg.getName(), metaCacheName)) {
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

            igfsCache.put(cfg0.getName(), igfsCtx);
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
            String dataCacheName = igfsCfg.getDataCacheConfiguration().getName();

            CacheConfiguration cacheCfg = cacheCfgs.get(dataCacheName);

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
                igfsCfg.getMetaCacheConfiguration().getName(),
                dataCacheName,
                igfsCfg.getDefaultMode(),
                igfsCfg.getPathModes(),
                igfsCfg.isFragmentizerEnabled()));
        }

        ctx.addNodeAttribute(ATTR_IGFS, attrVals.toArray(new IgfsAttributes[attrVals.size()]));
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart(boolean active) throws IgniteCheckedException {
        if (!active || ctx.config().isDaemon())
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
    @Override public void onActivate(GridKernalContext kctx) throws IgniteCheckedException {
        onKernalStart(true);
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) {
        onKernalStop(true);
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
        X.println(">>> IGFS processor memory stats [igniteInstanceName=" + ctx.igniteInstanceName() + ']');
        X.println(">>>   igfsCacheSize: " + igfsCache.size());
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteFileSystem> igfss() {
        return F.viewReadOnly(igfsCache.values(), CTX_TO_IGFS);
    }

    /** {@inheritDoc} */
    @Override @Nullable public IgniteFileSystem igfs(String name) {
        if (name == null)
            throw new IllegalArgumentException("IGFS name cannot be null");

        IgfsContext igfsCtx = igfsCache.get(name);

        return igfsCtx == null ? null : igfsCtx.igfs();
    }

    /** {@inheritDoc} */
    @Override @Nullable public Collection<IpcServerEndpoint> endpoints(String name) {
        if (name == null)
            throw new IllegalArgumentException("IGFS name cannot be null");

        IgfsContext igfsCtx = igfsCache.get(name);

        return igfsCtx == null ? Collections.<IpcServerEndpoint>emptyList() : igfsCtx.server().endpoints();
    }

    /** {@inheritDoc} */
    @Nullable @Override public ComputeJob createJob(IgfsJob job, @Nullable String igfsName, IgfsPath path,
        long start, long len, IgfsRecordResolver recRslv) {
        return new IgfsJobImpl(job, igfsName, path, start, len, recRslv);
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
