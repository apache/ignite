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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteComputeImpl;
import org.apache.ignite.internal.cluster.ClusterGroupAdapter;
import org.apache.ignite.internal.portable.PortableRawWriterEx;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.IgniteCacheProxy;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerImpl;
import org.apache.ignite.internal.processors.platform.cache.PlatformCache;
import org.apache.ignite.internal.processors.platform.cache.affinity.PlatformAffinity;
import org.apache.ignite.internal.processors.platform.cache.store.PlatformCacheStore;
import org.apache.ignite.internal.processors.platform.cluster.PlatformClusterGroup;
import org.apache.ignite.internal.processors.platform.compute.PlatformCompute;
import org.apache.ignite.internal.processors.platform.datastreamer.PlatformDataStreamer;
import org.apache.ignite.internal.processors.platform.dotnet.PlatformDotNetCacheStore;
import org.apache.ignite.internal.processors.platform.events.PlatformEvents;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.processors.platform.messaging.PlatformMessaging;
import org.apache.ignite.internal.processors.platform.services.PlatformServices;
import org.apache.ignite.internal.processors.platform.transactions.PlatformTransactions;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Collections;
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

    /** Started stores. */
    private final Collection<PlatformCacheStore> stores =
        Collections.newSetFromMap(new ConcurrentHashMap<PlatformCacheStore, Boolean>());

    /** Lock for store lifecycle operations. */
    private final ReadWriteLock storeLock = new ReentrantReadWriteLock();

    /** Logger. */
    private final IgniteLogger log;

    /** Context. */
    private final PlatformContext platformCtx;

    /** Interop configuration. */
    private final PlatformConfigurationEx interopCfg;

    /** Whether processor is started. */
    private boolean started;

    /** Whether processor if stopped (or stopping). */
    private boolean stopped;

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public PlatformProcessorImpl(GridKernalContext ctx) {
        super(ctx);

        log = ctx.log(PlatformProcessorImpl.class);

        PlatformConfiguration interopCfg0 = null; //ctx.config().getPlatformConfiguration();

        assert interopCfg0 != null : "Must be checked earlier during component creation.";

        if (!(interopCfg0 instanceof PlatformConfigurationEx))
            throw new IgniteException("Unsupported platform configuration: " + interopCfg0.getClass().getName());

        interopCfg = (PlatformConfigurationEx)interopCfg0;

        if (!F.isEmpty(interopCfg.warnings())) {
            for (String w : interopCfg.warnings())
                U.warn(log, w);
        }

        platformCtx = new PlatformContextImpl(ctx, interopCfg.gate(), interopCfg.memory());
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        try (PlatformMemory mem = platformCtx.memory().allocate()) {
            PlatformOutputStream out = mem.output();

            PortableRawWriterEx writer = platformCtx.writer(out);

            writer.writeString(ctx.gridName());

            out.synchronize();

            platformCtx.gateway().onStart(this, mem.pointer());
        }

        // At this moment all necessary native libraries must be loaded, so we can process with store creation.
        storeLock.writeLock().lock();

        try {
            for (StoreInfo store : pendingStores)
                registerStore0(store.store, store.convertPortable);

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
            // Destroy cache stores.
            storeLock.writeLock().lock();

            try {
                for (PlatformCacheStore store : stores) {
                    if (store != null) {
                        if (store instanceof PlatformDotNetCacheStore) {
                            PlatformDotNetCacheStore store0 = (PlatformDotNetCacheStore)store;

                            try {
                                store0.destroy(platformCtx.kernalContext());
                            }
                            catch (Exception e) {
                                U.error(log, "Failed to destroy .Net cache store [store=" + store0 +
                                    ", err=" + e.getMessage() + ']');
                            }
                        }
                        else
                            assert false : "Invalid interop cache store type: " + store;
                    }
                }
            }
            finally {
                stopped = true;

                storeLock.writeLock().unlock();
            }

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
    public void releaseStart() {
        startLatch.countDown();
    }

    /** {@inheritDoc} */
    public void awaitStart() throws IgniteCheckedException {
        U.await(startLatch);
    }

    /** {@inheritDoc} */
    @Override public PlatformContext context() {
        return platformCtx;
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget cache(@Nullable String name) throws IgniteCheckedException {
        IgniteCacheProxy cache = (IgniteCacheProxy)ctx.grid().cache(name);

        if (cache == null)
            throw new IllegalArgumentException("Cache doesn't exist: " + name);

        return new PlatformCache(platformCtx, cache.keepPortable(), false);
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget createCache(@Nullable String name) throws IgniteCheckedException {
        IgniteCacheProxy cache = (IgniteCacheProxy)ctx.grid().createCache(name);

        assert cache != null;

        return new PlatformCache(platformCtx, cache.keepPortable(), false);
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget getOrCreateCache(@Nullable String name) throws IgniteCheckedException {
        IgniteCacheProxy cache = (IgniteCacheProxy)ctx.grid().getOrCreateCache(name);

        assert cache != null;

        return new PlatformCache(platformCtx, cache.keepPortable(), false);
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget affinity(@Nullable String name) throws IgniteCheckedException {
        return new PlatformAffinity(platformCtx, ctx, name);
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget dataStreamer(@Nullable String cacheName, boolean keepPortable)
        throws IgniteCheckedException {
        IgniteDataStreamer ldr = ctx.dataStream().dataStreamer(cacheName);

        return new PlatformDataStreamer(platformCtx, cacheName, (DataStreamerImpl)ldr, keepPortable);
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget transactions() {
        return new PlatformTransactions(platformCtx);
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget projection() throws IgniteCheckedException {
        return new PlatformClusterGroup(platformCtx, ctx.grid().cluster());
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget compute(PlatformTarget grp) {
        PlatformClusterGroup grp0 = (PlatformClusterGroup)grp;

        assert grp0.projection() instanceof ClusterGroupAdapter; // Safety for very complex ClusterGroup hierarchy.

        return new PlatformCompute(platformCtx, (IgniteComputeImpl)((ClusterGroupAdapter)grp0.projection()).compute());
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget message(PlatformTarget grp) {
        PlatformClusterGroup grp0 = (PlatformClusterGroup)grp;

        return new PlatformMessaging(platformCtx, grp0.projection().ignite().message(grp0.projection()));
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget events(PlatformTarget grp) {
        PlatformClusterGroup grp0 = (PlatformClusterGroup)grp;

        return new PlatformEvents(platformCtx, grp0.projection().ignite().events(grp0.projection()));
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget services(PlatformTarget grp) {
        PlatformClusterGroup grp0 = (PlatformClusterGroup)grp;

        return new PlatformServices(platformCtx, grp0.projection().ignite().services(grp0.projection()), false);
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget extensions() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void registerStore(PlatformCacheStore store, boolean convertPortable)
        throws IgniteCheckedException {
        storeLock.readLock().lock();

        try {
            if (stopped)
                throw new IgniteCheckedException("Failed to initialize interop store becuase node is stopping: " +
                    store);

            if (started)
                registerStore0(store, convertPortable);
            else
                pendingStores.add(new StoreInfo(store, convertPortable));
        }
        finally {
            storeLock.readLock().unlock();
        }
    }

    /**
     * Internal store initialization routine.
     *
     * @param store Store.
     * @param convertPortable Convert portable flag.
     * @throws IgniteCheckedException If failed.
     */
    private void registerStore0(PlatformCacheStore store, boolean convertPortable) throws IgniteCheckedException {
        if (store instanceof PlatformDotNetCacheStore) {
            PlatformDotNetCacheStore store0 = (PlatformDotNetCacheStore)store;

            store0.initialize(ctx, convertPortable);
        }
        else
            throw new IgniteCheckedException("Unsupported interop store: " + store);
    }

    /**
     * Store and manager pair.
     */
    private static class StoreInfo {
        /** Store. */
        private final PlatformCacheStore store;

        /** Convert portable flag. */
        private final boolean convertPortable;

        /**
         * Constructor.
         *
         * @param store Store.
         * @param convertPortable Convert portable flag.
         */
        private StoreInfo(PlatformCacheStore store, boolean convertPortable) {
            this.store = store;
            this.convertPortable = convertPortable;
        }
    }
}