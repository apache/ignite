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

package org.apache.ignite.internal.processors.platform;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.configuration.PlatformConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.logger.platform.PlatformLogger;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerImpl;
import org.apache.ignite.internal.processors.datastructures.GridCacheAtomicLongImpl;
import org.apache.ignite.internal.processors.platform.binary.PlatformBinaryProcessor;
import org.apache.ignite.internal.processors.platform.cache.PlatformCache;
import org.apache.ignite.internal.processors.platform.cache.PlatformCacheExtension;
import org.apache.ignite.internal.processors.platform.cache.affinity.PlatformAffinity;
import org.apache.ignite.internal.processors.platform.cache.store.PlatformCacheStore;
import org.apache.ignite.internal.processors.platform.cluster.PlatformClusterGroup;
import org.apache.ignite.internal.processors.platform.compute.PlatformCompute;
import org.apache.ignite.internal.processors.platform.datastreamer.PlatformDataStreamer;
import org.apache.ignite.internal.processors.platform.datastructures.PlatformAtomicLong;
import org.apache.ignite.internal.processors.platform.datastructures.PlatformAtomicReference;
import org.apache.ignite.internal.processors.platform.datastructures.PlatformAtomicSequence;
import org.apache.ignite.internal.processors.platform.dotnet.PlatformDotNetCacheStore;
import org.apache.ignite.internal.processors.platform.events.PlatformEvents;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.processors.platform.messaging.PlatformMessaging;
import org.apache.ignite.internal.processors.platform.services.PlatformServices;
import org.apache.ignite.internal.processors.platform.transactions.PlatformTransactions;
import org.apache.ignite.internal.processors.platform.utils.PlatformConfigurationUtils;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * GridGain platform processor.
 */
public class PlatformProcessorImpl extends GridProcessorAdapter implements PlatformProcessor {
    /** Start latch. */
    private final CountDownLatch startLatch = new CountDownLatch(1);

    /** Stores pending initialization. */
    private final Collection<StoreInfo> pendingStores =
        Collections.newSetFromMap(new ConcurrentHashMap<StoreInfo, Boolean>());

    /** Lock for store lifecycle operations. */
    private final ReadWriteLock storeLock = new ReentrantReadWriteLock();

    /** Logger. */
    @SuppressWarnings("FieldCanBeLocal")
    private final IgniteLogger log;

    /** Context. */
    private final PlatformContext platformCtx;

    /** Interop configuration. */
    private final PlatformConfigurationEx interopCfg;

    /** Whether processor is started. */
    private boolean started;

    /** Whether processor if stopped (or stopping). */
    private volatile boolean stopped;

    /** Cache extensions. */
    private final PlatformCacheExtension[] cacheExts;

    /** Cluster restart flag for the reconnect callback. */
    private volatile boolean clusterRestarted;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public PlatformProcessorImpl(GridKernalContext ctx) {
        super(ctx);

        log = ctx.log(PlatformProcessorImpl.class);

        PlatformConfiguration interopCfg0 = ctx.config().getPlatformConfiguration();

        assert interopCfg0 != null : "Must be checked earlier during component creation.";

        if (!(interopCfg0 instanceof PlatformConfigurationEx))
            throw new IgniteException("Unsupported platform configuration: " + interopCfg0.getClass().getName());

        interopCfg = (PlatformConfigurationEx)interopCfg0;

        if (!F.isEmpty(interopCfg.warnings())) {
            for (String w : interopCfg.warnings())
                U.warn(log, w);
        }

        platformCtx = new PlatformContextImpl(ctx, interopCfg.gate(), interopCfg.memory(), interopCfg.platform());

        // Initialize cache extensions (if any).
        cacheExts = prepareCacheExtensions(interopCfg.cacheExtensions());

        if (interopCfg.logger() != null)
            interopCfg.logger().setContext(platformCtx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        try (PlatformMemory mem = platformCtx.memory().allocate()) {
            PlatformOutputStream out = mem.output();

            BinaryRawWriterEx writer = platformCtx.writer(out);

            writer.writeString(ctx.gridName());

            out.synchronize();

            platformCtx.gateway().onStart(this, mem.pointer());
        }

        // At this moment all necessary native libraries must be loaded, so we can process with store creation.
        storeLock.writeLock().lock();

        try {
            for (StoreInfo store : pendingStores)
                registerStore0(store.store, store.convertBinary);

            pendingStores.clear();

            started = true;
        }
        finally {
            storeLock.writeLock().unlock();
        }

        // Add Interop node attributes.
        ctx.addNodeAttribute(PlatformUtils.ATTR_PLATFORM, interopCfg.platform());
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        startLatch.countDown();
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        if (platformCtx != null) {
            stopped = true;
            platformCtx.gateway().onStop();
        }
    }

    /** {@inheritDoc} */
    @Override public Ignite ignite() {
        return ctx.grid();
    }

    /** {@inheritDoc} */
    @Override public long environmentPointer() {
        return platformCtx.gateway().environmentPointer();
    }

    /** {@inheritDoc} */
    @Override public void releaseStart() {
        startLatch.countDown();
    }

    /** {@inheritDoc} */
    @Override public void awaitStart() throws IgniteCheckedException {
        U.await(startLatch);
    }

    /** {@inheritDoc} */
    @Override public PlatformContext context() {
        // This method is a single point of entry for all remote closures
        // CPP platform does not currently support remote code execution
        // Therefore, all remote execution attempts come from .NET
        // Throw an error if current platform is not .NET
        if (!PlatformUtils.PLATFORM_DOTNET.equals(interopCfg.platform())) {
            throw new IgniteException(".NET platform is not available [nodeId=" + ctx.grid().localNode().id() + "] " +
                "(Use Apache.Ignite.Core.Ignition.Start() or Apache.Ignite.exe to start Ignite.NET nodes).");
        }

        return platformCtx;
    }

    /** {@inheritDoc} */
    @Override public PlatformTargetProxy cache(@Nullable String name) throws IgniteCheckedException {
        IgniteCacheProxy cache = (IgniteCacheProxy)ctx.grid().cache(name);

        if (cache == null)
            throw new IllegalArgumentException("Cache doesn't exist: " + name);

        return createPlatformCache(cache);
    }

    /** {@inheritDoc} */
    @Override public PlatformTargetProxy createCache(@Nullable String name) throws IgniteCheckedException {
        IgniteCacheProxy cache = (IgniteCacheProxy)ctx.grid().createCache(name);

        assert cache != null;

        return createPlatformCache(cache);
    }

    /** {@inheritDoc} */
    @Override public PlatformTargetProxy getOrCreateCache(@Nullable String name) throws IgniteCheckedException {
        IgniteCacheProxy cache = (IgniteCacheProxy)ctx.grid().getOrCreateCache(name);

        assert cache != null;

        return createPlatformCache(cache);
    }

    /** {@inheritDoc} */
    @Override public PlatformTargetProxy createCacheFromConfig(long memPtr) throws IgniteCheckedException {
        BinaryRawReaderEx reader = platformCtx.reader(platformCtx.memory().get(memPtr));
        CacheConfiguration cfg = PlatformConfigurationUtils.readCacheConfiguration(reader);

        IgniteCacheProxy cache = reader.readBoolean()
            ? (IgniteCacheProxy)ctx.grid().createCache(cfg, PlatformConfigurationUtils.readNearConfiguration(reader))
            : (IgniteCacheProxy)ctx.grid().createCache(cfg);

        return createPlatformCache(cache);
    }

    /** {@inheritDoc} */
    @Override public PlatformTargetProxy getOrCreateCacheFromConfig(long memPtr) throws IgniteCheckedException {
        BinaryRawReaderEx reader = platformCtx.reader(platformCtx.memory().get(memPtr));
        CacheConfiguration cfg = PlatformConfigurationUtils.readCacheConfiguration(reader);

        IgniteCacheProxy cache = reader.readBoolean()
            ? (IgniteCacheProxy)ctx.grid().getOrCreateCache(cfg,
                    PlatformConfigurationUtils.readNearConfiguration(reader))
            : (IgniteCacheProxy)ctx.grid().getOrCreateCache(cfg);

        return createPlatformCache(cache);
    }

    /** {@inheritDoc} */
    @Override public void destroyCache(@Nullable String name) throws IgniteCheckedException {
        ctx.grid().destroyCache(name);
    }

    /** {@inheritDoc} */
    @Override public PlatformTargetProxy affinity(@Nullable String name) throws IgniteCheckedException {
        return proxy(new PlatformAffinity(platformCtx, ctx, name));
    }

    /** {@inheritDoc} */
    @Override public PlatformTargetProxy dataStreamer(@Nullable String cacheName, boolean keepBinary)
        throws IgniteCheckedException {
        IgniteDataStreamer ldr = ctx.dataStream().dataStreamer(cacheName);

        ldr.keepBinary(true);

        return proxy(new PlatformDataStreamer(platformCtx, cacheName, (DataStreamerImpl)ldr, keepBinary));
    }

    /** {@inheritDoc} */
    @Override public PlatformTargetProxy transactions() {
        return proxy(new PlatformTransactions(platformCtx));
    }

    /** {@inheritDoc} */
    @Override public PlatformTargetProxy projection() throws IgniteCheckedException {
        return proxy(new PlatformClusterGroup(platformCtx, ctx.grid().cluster()));
    }

    /** {@inheritDoc} */
    @Override public PlatformTargetProxy compute(PlatformTargetProxy grp) {
        PlatformClusterGroup grp0 = (PlatformClusterGroup)grp.unwrap();

        return proxy(new PlatformCompute(platformCtx, grp0.projection(), PlatformUtils.ATTR_PLATFORM));
    }

    /** {@inheritDoc} */
    @Override public PlatformTargetProxy message(PlatformTargetProxy grp) {
        PlatformClusterGroup grp0 = (PlatformClusterGroup)grp.unwrap();

        return proxy(new PlatformMessaging(platformCtx, grp0.projection().ignite().message(grp0.projection())));
    }

    /** {@inheritDoc} */
    @Override public PlatformTargetProxy events(PlatformTargetProxy grp) {
        PlatformClusterGroup grp0 = (PlatformClusterGroup)grp.unwrap();

        return proxy(new PlatformEvents(platformCtx, grp0.projection().ignite().events(grp0.projection())));
    }

    /** {@inheritDoc} */
    @Override public PlatformTargetProxy services(PlatformTargetProxy grp) {
        PlatformClusterGroup grp0 = (PlatformClusterGroup)grp.unwrap();

        return proxy(new PlatformServices(platformCtx, grp0.projection().ignite().services(grp0.projection()), false));
    }

    /** {@inheritDoc} */
    @Override public PlatformTargetProxy extensions() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void registerStore(PlatformCacheStore store, boolean convertBinary)
        throws IgniteCheckedException {
        storeLock.readLock().lock();

        try {
            if (stopped)
                throw new IgniteCheckedException("Failed to initialize interop store because node is stopping: " +
                    store);

            if (started)
                registerStore0(store, convertBinary);
            else
                pendingStores.add(new StoreInfo(store, convertBinary));
        }
        finally {
            storeLock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public PlatformTargetProxy atomicLong(String name, long initVal, boolean create) throws IgniteException {
        GridCacheAtomicLongImpl atomicLong = (GridCacheAtomicLongImpl)ignite().atomicLong(name, initVal, create);

        if (atomicLong == null)
            return null;

        return proxy(new PlatformAtomicLong(platformCtx, atomicLong));
    }

    /** {@inheritDoc} */
    @Override public PlatformTargetProxy atomicSequence(String name, long initVal, boolean create)
        throws IgniteException {
        IgniteAtomicSequence atomicSeq = ignite().atomicSequence(name, initVal, create);

        if (atomicSeq == null)
            return null;

        return proxy(new PlatformAtomicSequence(platformCtx, atomicSeq));
    }

    /** {@inheritDoc} */
    @Override public PlatformTargetProxy atomicReference(String name, long memPtr, boolean create)
        throws IgniteException {
        PlatformAtomicReference ref = PlatformAtomicReference.createInstance(platformCtx, name, memPtr, create);

        return ref != null ? proxy(ref) : null;
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) throws IgniteCheckedException {
        platformCtx.gateway().onClientDisconnected();

        // 1) onReconnected is called on all grid components.
        // 2) After all of grid components have completed their reconnection, reconnectFut is completed.
        reconnectFut.listen(new CI1<IgniteFuture<?>>() {
            @Override public void apply(IgniteFuture<?> future) {
                platformCtx.gateway().onClientReconnected(clusterRestarted);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> onReconnected(boolean clusterRestarted) throws IgniteCheckedException {
        // Save the flag value for callback of reconnectFut.
        this.clusterRestarted = clusterRestarted;

        return null;
    }

    /** {@inheritDoc} */
    @Override public void getIgniteConfiguration(long memPtr) {
        PlatformOutputStream stream = platformCtx.memory().get(memPtr).output();
        BinaryRawWriterEx writer = platformCtx.writer(stream);

        PlatformConfigurationUtils.writeIgniteConfiguration(writer, ignite().configuration());

        stream.synchronize();
    }

    /** {@inheritDoc} */
    @Override public void getCacheNames(long memPtr) {
        PlatformOutputStream stream = platformCtx.memory().get(memPtr).output();
        BinaryRawWriterEx writer = platformCtx.writer(stream);

        Collection<String> names = ignite().cacheNames();

        writer.writeInt(names.size());

        for (String name : names)
            writer.writeString(name);

        stream.synchronize();
    }

    /** {@inheritDoc} */
    @Override public PlatformTargetProxy createNearCache(@Nullable String cacheName, long memPtr) {
        NearCacheConfiguration cfg = getNearCacheConfiguration(memPtr);

        IgniteCacheProxy cache = (IgniteCacheProxy)ctx.grid().createNearCache(cacheName, cfg);

        return createPlatformCache(cache);
    }

    /** {@inheritDoc} */
    @Override public PlatformTargetProxy getOrCreateNearCache(@Nullable String cacheName, long memPtr) {
        NearCacheConfiguration cfg = getNearCacheConfiguration(memPtr);

        IgniteCacheProxy cache = (IgniteCacheProxy)ctx.grid().getOrCreateNearCache(cacheName, cfg);

        return createPlatformCache(cache);
    }

    /**
     * Creates new platform cache.
     */
    private PlatformTargetProxy createPlatformCache(IgniteCacheProxy cache) {
        return proxy(new PlatformCache(platformCtx, cache, false, cacheExts));
    }

    /** {@inheritDoc} */
    @Override public boolean loggerIsLevelEnabled(int level) {
        IgniteLogger log = ctx.grid().log();

        switch (level) {
            case PlatformLogger.LVL_TRACE:
                return log.isTraceEnabled();
            case PlatformLogger.LVL_DEBUG:
                return log.isDebugEnabled();
            case PlatformLogger.LVL_INFO:
                return log.isInfoEnabled();
            case PlatformLogger.LVL_WARN:
                return true;
            case PlatformLogger.LVL_ERROR:
                return true;
            default:
                assert false;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public void loggerLog(int level, String message, String category, String errorInfo) {
        IgniteLogger log = ctx.grid().log();

        if (category != null)
            log = log.getLogger(category);

        Throwable err = errorInfo == null ? null : new IgniteException("Platform error:" + errorInfo);

        switch (level) {
            case PlatformLogger.LVL_TRACE:
                log.trace(message);
                break;
            case PlatformLogger.LVL_DEBUG:
                log.debug(message);
                break;
            case PlatformLogger.LVL_INFO:
                log.info(message);
                break;
            case PlatformLogger.LVL_WARN:
                log.warning(message, err);
                break;
            case PlatformLogger.LVL_ERROR:
                log.error(message, err);
                break;
            default:
                assert false;
        }
    }

    /** {@inheritDoc} */
    @Override public PlatformTargetProxy binaryProcessor() {
        return proxy(new PlatformBinaryProcessor(platformCtx));
    }

    /**
     * Gets the near cache config.
     *
     * @param memPtr Memory pointer.
     * @return Near config.
     */
    private NearCacheConfiguration getNearCacheConfiguration(long memPtr) {
        assert memPtr != 0;

        BinaryRawReaderEx reader = platformCtx.reader(platformCtx.memory().get(memPtr));
        return PlatformConfigurationUtils.readNearConfiguration(reader);
    }

    /**
     * Internal store initialization routine.
     *
     * @param store Store.
     * @param convertBinary Convert binary flag.
     * @throws IgniteCheckedException If failed.
     */
    private void registerStore0(PlatformCacheStore store, boolean convertBinary) throws IgniteCheckedException {
        if (store instanceof PlatformDotNetCacheStore) {
            PlatformDotNetCacheStore store0 = (PlatformDotNetCacheStore)store;

            store0.initialize(ctx, convertBinary);
        }
        else
            throw new IgniteCheckedException("Unsupported interop store: " + store);
    }

    /**
     * Prepare cache extensions.
     *
     * @param cacheExts Original extensions.
     * @return Prepared extensions.
     */
    private static PlatformCacheExtension[] prepareCacheExtensions(Collection<PlatformCacheExtension> cacheExts) {
        if (!F.isEmpty(cacheExts)) {
            int maxExtId = 0;

            Map<Integer, PlatformCacheExtension> idToExt = new HashMap<>();

            for (PlatformCacheExtension cacheExt : cacheExts) {
                if (cacheExt == null)
                    throw new IgniteException("Platform cache extension cannot be null.");

                if (cacheExt.id() < 0)
                    throw new IgniteException("Platform cache extension ID cannot be negative: " + cacheExt);

                PlatformCacheExtension oldCacheExt = idToExt.put(cacheExt.id(), cacheExt);

                if (oldCacheExt != null)
                    throw new IgniteException("Platform cache extensions cannot have the same ID [" +
                        "id=" + cacheExt.id() + ", first=" + oldCacheExt + ", second=" + cacheExt + ']');

                if (cacheExt.id() > maxExtId)
                    maxExtId = cacheExt.id();
            }

            PlatformCacheExtension[] res = new PlatformCacheExtension[maxExtId + 1];

            for (PlatformCacheExtension cacheExt : cacheExts)
                res[cacheExt.id()]= cacheExt;

            return res;
        }
        else
            //noinspection ZeroLengthArrayAllocation
            return new PlatformCacheExtension[0];
    }

    /**
     * Wraps target in a proxy.
     */
    private PlatformTargetProxy proxy(PlatformTarget target) {
        return new PlatformTargetProxyImpl(target, platformCtx);
    }

    /**
     * Store and manager pair.
     */
    private static class StoreInfo {
        /** Store. */
        private final PlatformCacheStore store;

        /** Convert binary flag. */
        private final boolean convertBinary;

        /**
         * Constructor.
         *
         * @param store Store.
         * @param convertBinary Convert binary flag.
         */
        private StoreInfo(PlatformCacheStore store, boolean convertBinary) {
            this.store = store;
            this.convertBinary = convertBinary;
        }
    }
}
