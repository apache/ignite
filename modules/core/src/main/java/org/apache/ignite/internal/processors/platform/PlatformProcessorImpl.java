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
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.configuration.PlatformConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.cluster.DetachedClusterNode;
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
import org.apache.ignite.internal.processors.platform.datastreamer.PlatformDataStreamer;
import org.apache.ignite.internal.processors.platform.datastructures.PlatformAtomicLong;
import org.apache.ignite.internal.processors.platform.datastructures.PlatformAtomicReference;
import org.apache.ignite.internal.processors.platform.datastructures.PlatformAtomicSequence;
import org.apache.ignite.internal.processors.platform.dotnet.PlatformDotNetCacheStore;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.processors.platform.transactions.PlatformTransactions;
import org.apache.ignite.internal.processors.platform.utils.PlatformConfigurationUtils;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.ignite.internal.processors.platform.PlatformAbstractTarget.FALSE;
import static org.apache.ignite.internal.processors.platform.PlatformAbstractTarget.TRUE;
import static org.apache.ignite.internal.processors.platform.client.ClientConnectionContext.CURRENT_VER;

/**
 * GridGain platform processor.
 */
@SuppressWarnings({"ConditionalExpressionWithIdenticalBranches", "unchecked"})
public class PlatformProcessorImpl extends GridProcessorAdapter implements PlatformProcessor, PlatformTarget {
    /** */
    private static final int OP_GET_CACHE = 1;

    /** */
    private static final int OP_CREATE_CACHE = 2;

    /** */
    private static final int OP_GET_OR_CREATE_CACHE = 3;

    /** */
    private static final int OP_CREATE_CACHE_FROM_CONFIG = 4;

    /** */
    private static final int OP_GET_OR_CREATE_CACHE_FROM_CONFIG = 5;

    /** */
    private static final int OP_DESTROY_CACHE = 6;

    /** */
    private static final int OP_GET_AFFINITY = 7;

    /** */
    private static final int OP_GET_DATA_STREAMER = 8;

    /** */
    private static final int OP_GET_TRANSACTIONS = 9;

    /** */
    private static final int OP_GET_CLUSTER_GROUP = 10;

    /** */
    private static final int OP_GET_EXTENSION = 11;

    /** */
    private static final int OP_GET_ATOMIC_LONG = 12;

    /** */
    private static final int OP_GET_ATOMIC_REFERENCE = 13;

    /** */
    private static final int OP_GET_ATOMIC_SEQUENCE = 14;

    /** */
    private static final int OP_GET_IGNITE_CONFIGURATION = 15;

    /** */
    private static final int OP_GET_CACHE_NAMES = 16;

    /** */
    private static final int OP_CREATE_NEAR_CACHE = 17;

    /** */
    private static final int OP_GET_OR_CREATE_NEAR_CACHE = 18;

    /** */
    private static final int OP_LOGGER_IS_LEVEL_ENABLED = 19;

    /** */
    private static final int OP_LOGGER_LOG = 20;

    /** */
    private static final int OP_GET_BINARY_PROCESSOR = 21;

    /** */
    private static final int OP_RELEASE_START = 22;

    /** */
    private static final int OP_ADD_CACHE_CONFIGURATION = 23;

    /** */
    private static final int OP_SET_BASELINE_TOPOLOGY_VER = 24;

    /** */
    private static final int OP_SET_BASELINE_TOPOLOGY_NODES = 25;

    /** */
    private static final int OP_GET_BASELINE_TOPOLOGY = 26;

    /** */
    private static final int OP_DISABLE_WAL = 27;

    /** */
    private static final int OP_ENABLE_WAL = 28;

    /** */
    private static final int OP_IS_WAL_ENABLED = 29;

    /** */
    private static final int OP_SET_TX_TIMEOUT_ON_PME = 30;

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

    /** Extensions. */
    private final PlatformPluginExtension[] extensions;

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

        // Initialize extensions (if any).
        extensions = prepareExtensions(ctx.plugins().extensions(PlatformPluginExtension.class));
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        try (PlatformMemory mem = platformCtx.memory().allocate()) {
            PlatformOutputStream out = mem.output();

            BinaryRawWriterEx writer = platformCtx.writer(out);

            writer.writeString(ctx.igniteInstanceName());

            out.synchronize();

            platformCtx.gateway().onStart(new PlatformTargetProxyImpl(this, platformCtx), mem.pointer());
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
        return platformCtx;
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

    /**
     * Creates new platform cache.
     */
    private PlatformTarget createPlatformCache(IgniteCacheProxy cache) {
        assert cache != null;

        return new PlatformCache(platformCtx, cache, false, cacheExts);
    }

    /**
     * Checks whether logger level is enabled.
     *
     * @param level Level.
     * @return Result.
     */
    private boolean loggerIsLevelEnabled(int level) {
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

    /**
     * Logs to the Ignite logger.
     *
     * @param level Level.
     * @param message Message.
     * @param category Category.
     * @param errorInfo Exception.
     */
    private void loggerLog(int level, String message, String category, String errorInfo) {
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
    @Override public long processInLongOutLong(int type, long val) throws IgniteCheckedException {
        switch (type) {
            case OP_LOGGER_IS_LEVEL_ENABLED: {
                return loggerIsLevelEnabled((int) val) ? TRUE : FALSE;
            }

            case OP_RELEASE_START: {
                releaseStart();

                return 0;
            }

            case OP_SET_BASELINE_TOPOLOGY_VER: {
                ctx.grid().cluster().setBaselineTopology(val);

                return 0;
            }
        }

        return PlatformAbstractTarget.throwUnsupported(type);
    }

    /** {@inheritDoc} */
    @Override public long processInStreamOutLong(int type, BinaryRawReaderEx reader) throws IgniteCheckedException {
        switch (type) {
            case OP_DESTROY_CACHE: {
                ctx.grid().destroyCache(reader.readString());

                return 0;
            }

            case OP_LOGGER_LOG: {
                loggerLog(reader.readInt(), reader.readString(), reader.readString(), reader.readString());

                return 0;
            }

            case OP_SET_BASELINE_TOPOLOGY_NODES: {
                int cnt = reader.readInt();
                Collection<BaselineNode> nodes = new ArrayList<>(cnt);

                for (int i = 0; i < cnt; i++) {
                    Object consId = reader.readObjectDetached();
                    Map<String, Object> attrs = PlatformUtils.readNodeAttributes(reader);

                    nodes.add(new DetachedClusterNode(consId, attrs));
                }

                ctx.grid().cluster().setBaselineTopology(nodes);

                return 0;
            }

            case OP_ADD_CACHE_CONFIGURATION:
                CacheConfiguration cfg = PlatformConfigurationUtils.readCacheConfiguration(reader, CURRENT_VER);

                ctx.grid().addCacheConfiguration(cfg);

                return 0;

            case OP_DISABLE_WAL:
                ctx.grid().cluster().disableWal(reader.readString());

                return 0;

            case OP_ENABLE_WAL:
                ctx.grid().cluster().enableWal(reader.readString());

                return 0;

            case OP_SET_TX_TIMEOUT_ON_PME:
                ctx.grid().cluster().setTxTimeoutOnPartitionMapExchange(reader.readLong());

                return 0;

            case OP_IS_WAL_ENABLED:
                return ctx.grid().cluster().isWalEnabled(reader.readString()) ? TRUE : FALSE;
        }

        return PlatformAbstractTarget.throwUnsupported(type);
    }

    /** {@inheritDoc} */
    @Override public long processInStreamOutLong(int type, BinaryRawReaderEx reader, PlatformMemory mem) throws IgniteCheckedException {
        return processInStreamOutLong(type, reader);
    }

    /** {@inheritDoc} */
    @Override public void processInStreamOutStream(int type, BinaryRawReaderEx reader, BinaryRawWriterEx writer) throws IgniteCheckedException {
        PlatformAbstractTarget.throwUnsupported(type);
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget processInStreamOutObject(int type, BinaryRawReaderEx reader) throws IgniteCheckedException {
        switch (type) {
            case OP_GET_CACHE: {
                String name = reader.readString();

                IgniteCacheProxy cache = (IgniteCacheProxy)ctx.grid().cache(name);

                if (cache == null)
                    throw new IllegalArgumentException("Cache doesn't exist: " + name);

                return createPlatformCache(cache);
            }

            case OP_CREATE_CACHE: {
                String name = reader.readString();

                IgniteCacheProxy cache = (IgniteCacheProxy)ctx.grid().createCache(name);

                return createPlatformCache(cache);
            }

            case OP_GET_OR_CREATE_CACHE: {
                String name = reader.readString();

                IgniteCacheProxy cache = (IgniteCacheProxy)ctx.grid().getOrCreateCache(name);

                return createPlatformCache(cache);
            }

            case OP_CREATE_CACHE_FROM_CONFIG: {
                CacheConfiguration cfg = PlatformConfigurationUtils.readCacheConfiguration(reader, CURRENT_VER);

                IgniteCacheProxy cache = reader.readBoolean()
                        ? (IgniteCacheProxy)ctx.grid().createCache(cfg, PlatformConfigurationUtils.readNearConfiguration(reader))
                        : (IgniteCacheProxy)ctx.grid().createCache(cfg);

                return createPlatformCache(cache);
            }

            case OP_GET_OR_CREATE_CACHE_FROM_CONFIG: {
                CacheConfiguration cfg = PlatformConfigurationUtils.readCacheConfiguration(reader, CURRENT_VER);

                IgniteCacheProxy cache = reader.readBoolean()
                        ? (IgniteCacheProxy)ctx.grid().getOrCreateCache(cfg,
                        PlatformConfigurationUtils.readNearConfiguration(reader))
                        : (IgniteCacheProxy)ctx.grid().getOrCreateCache(cfg);

                return createPlatformCache(cache);
            }

            case OP_GET_AFFINITY: {
                return new PlatformAffinity(platformCtx, ctx, reader.readString());
            }

            case OP_GET_DATA_STREAMER: {
                String cacheName = reader.readString();
                boolean keepBinary = reader.readBoolean();

                IgniteDataStreamer ldr = ctx.dataStream().dataStreamer(cacheName);

                ldr.keepBinary(true);

                return new PlatformDataStreamer(platformCtx, cacheName, (DataStreamerImpl)ldr, keepBinary);
            }

            case OP_GET_EXTENSION: {
                int id = reader.readInt();

                if (extensions != null && id < extensions.length) {
                    PlatformPluginExtension ext = extensions[id];

                    if (ext != null) {
                        return ext.createTarget();
                    }
                }

                throw new IgniteException("Platform extension is not registered [id=" + id + ']');
            }

            case OP_GET_ATOMIC_LONG: {
                String name = reader.readString();
                long initVal = reader.readLong();
                boolean create = reader.readBoolean();

                GridCacheAtomicLongImpl atomicLong = (GridCacheAtomicLongImpl)ignite().atomicLong(name, initVal, create);

                if (atomicLong == null)
                    return null;

                return new PlatformAtomicLong(platformCtx, atomicLong);
            }

            case OP_GET_ATOMIC_REFERENCE: {
                String name = reader.readString();
                Object initVal = reader.readObjectDetached();
                boolean create = reader.readBoolean();

                return PlatformAtomicReference.createInstance(platformCtx, name, initVal, create);
            }

            case OP_GET_ATOMIC_SEQUENCE: {
                String name = reader.readString();
                long initVal = reader.readLong();
                boolean create = reader.readBoolean();

                IgniteAtomicSequence atomicSeq = ignite().atomicSequence(name, initVal, create);

                if (atomicSeq == null)
                    return null;

                return new PlatformAtomicSequence(platformCtx, atomicSeq);
            }

            case OP_CREATE_NEAR_CACHE: {
                String cacheName = reader.readString();

                NearCacheConfiguration cfg = PlatformConfigurationUtils.readNearConfiguration(reader);

                IgniteCacheProxy cache = (IgniteCacheProxy)ctx.grid().createNearCache(cacheName, cfg);

                return createPlatformCache(cache);
            }

            case OP_GET_OR_CREATE_NEAR_CACHE: {
                String cacheName = reader.readString();

                NearCacheConfiguration cfg = PlatformConfigurationUtils.readNearConfiguration(reader);

                IgniteCacheProxy cache = (IgniteCacheProxy)ctx.grid().getOrCreateNearCache(cacheName, cfg);

                return createPlatformCache(cache);
            }

            case OP_GET_TRANSACTIONS: {
                String lbl = reader.readString();

                return new PlatformTransactions(platformCtx, lbl);
            }
        }

        return PlatformAbstractTarget.throwUnsupported(type);
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget processInObjectStreamOutObjectStream(int type, @Nullable PlatformTarget arg,
                                                                         BinaryRawReaderEx reader,
                                                                         BinaryRawWriterEx writer)
            throws IgniteCheckedException {
        return PlatformAbstractTarget.throwUnsupported(type);
    }

    /** {@inheritDoc} */
    @Override public void processOutStream(int type, BinaryRawWriterEx writer) throws IgniteCheckedException {
        switch (type) {
            case OP_GET_IGNITE_CONFIGURATION: {
                PlatformConfigurationUtils.writeIgniteConfiguration(writer, ignite().configuration(), CURRENT_VER);

                return;
            }

            case OP_GET_CACHE_NAMES: {
                Collection<String> names = ignite().cacheNames();

                writer.writeInt(names.size());

                for (String name : names)
                    writer.writeString(name);

                return;
            }

            case OP_GET_BASELINE_TOPOLOGY: {
                Collection<BaselineNode> blt = ignite().cluster().currentBaselineTopology();
                writer.writeInt(blt.size());

                for (BaselineNode n : blt) {
                    writer.writeObjectDetached(n.consistentId());
                    PlatformUtils.writeNodeAttributes(writer, n.attributes());
                }

                return;
            }
        }

        PlatformAbstractTarget.throwUnsupported(type);
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget processOutObject(int type) throws IgniteCheckedException {
        switch (type) {
            case OP_GET_TRANSACTIONS:
                return new PlatformTransactions(platformCtx);

            case OP_GET_CLUSTER_GROUP:
                return new PlatformClusterGroup(platformCtx, ctx.grid().cluster());

            case OP_GET_BINARY_PROCESSOR: {
                return new PlatformBinaryProcessor(platformCtx);
            }
        }

        return PlatformAbstractTarget.throwUnsupported(type);
    }

    /** {@inheritDoc} */
    @Override public PlatformAsyncResult processInStreamAsync(int type, BinaryRawReaderEx reader) throws IgniteCheckedException {
        return PlatformAbstractTarget.throwUnsupported(type);
    }

    /** {@inheritDoc} */
    @Override public Exception convertException(Exception e) {
        return e;
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
     * Prepare extensions.
     *
     * @param exts Original extensions.
     * @return Prepared extensions.
     */
    private static PlatformPluginExtension[] prepareExtensions(PlatformPluginExtension[] exts) {
        if (!F.isEmpty(exts)) {
            int maxExtId = 0;

            Map<Integer, PlatformPluginExtension> idToExt = new HashMap<>();

            for (PlatformPluginExtension ext : exts) {
                if (ext == null)
                    throw new IgniteException("Platform extension cannot be null.");

                if (ext.id() < 0)
                    throw new IgniteException("Platform extension ID cannot be negative: " + ext);

                PlatformPluginExtension oldCacheExt = idToExt.put(ext.id(), ext);

                if (oldCacheExt != null)
                    throw new IgniteException("Platform extensions cannot have the same ID [" +
                            "id=" + ext.id() + ", first=" + oldCacheExt + ", second=" + ext + ']');

                if (ext.id() > maxExtId)
                    maxExtId = ext.id();
            }

            PlatformPluginExtension[] res = new PlatformPluginExtension[maxExtId + 1];

            for (PlatformPluginExtension ext : exts)
                res[ext.id()]= ext;

            return res;
        }
        else
            //noinspection ZeroLengthArrayAllocation
            return new PlatformPluginExtension[0];
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
