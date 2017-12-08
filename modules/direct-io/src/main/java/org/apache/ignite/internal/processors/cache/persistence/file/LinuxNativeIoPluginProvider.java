/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.file;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.CachePluginContext;
import org.apache.ignite.plugin.CachePluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.PluginValidationException;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

/**
 * Plugin provider for setting up {@link IgniteNativeIoLib}.
 */
public class LinuxNativeIoPluginProvider implements PluginProvider {
    /** Managed buffers map from address to thread requested buffer. */
    @Nullable private ConcurrentHashMap8<Long, Thread> managedBuffers;

    /** Logger. */
    private IgniteLogger log;
    private String name;

    /** {@inheritDoc} */
    @Override public String name() {
        return "Ignite Native I/O Plugin [Direct I/O]";
    }

    /** {@inheritDoc} */
    @Override public String version() {
        return "";
    }

    /** {@inheritDoc} */
    @Override public String copyright() {
        return "Copyright(C) Apache Software Foundation";
    }

    /** {@inheritDoc} */
    @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public CachePluginProvider createCacheProvider(CachePluginContext ctx) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void start(PluginContext ctx) throws IgniteCheckedException {
        final Ignite ignite = ctx.grid();

        log = ignite.log();
        name = ignite.name();
        managedBuffers = setupDirect((IgniteEx)ignite);
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        freeDirectBuffers();
    }

    /**
     * Free direct thread local buffer allocated for Direct IO user's threads.
     */
    private void freeDirectBuffers() {
        if (managedBuffers == null)
            return;

        List<T2<Long, Thread>> remainedBuffers = new ArrayList<>(managedBuffers.size());

        for (Map.Entry<Long, Thread> next : managedBuffers.entrySet()) {
            remainedBuffers.add(new T2<>(next.getKey(), next.getValue()));
        }
        managedBuffers.clear();

        if (log.isInfoEnabled())
            log.info("Direct IO buffers to be freed: " + remainedBuffers.size());

        final List<T2<Long, Thread>> remainedForDaemon = cleanBuffers(remainedBuffers);

        if (F.isEmpty(remainedForDaemon))
            return;

        Runnable runnable = new Runnable() {
            @Override public void run() {
                List<T2<Long, Thread>> remainedBuffers = remainedForDaemon;
                do {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));

                    remainedBuffers = cleanBuffers(remainedBuffers);
                }
                while (!F.isEmpty(remainedBuffers));
            }
        };

        Thread cleanupDeamon = new Thread(runnable);

        cleanupDeamon.setName("native-buffer-cleanup" + (this.name == null ? "" : "-" + this.name));
        cleanupDeamon.setDaemon(true);
        cleanupDeamon.start();
    }

    /**
     * @param buf list of remained references
     * @return remained buffer pointers or null
     */
    @Nullable private List<T2<Long, Thread>> cleanBuffers(@Nullable List<T2<Long, Thread>> buf) {
        if (buf == null)
            return null;

        List<T2<Long, Thread>> remained = null;
        for (T2<Long, Thread> next : buf) {
            Thread th = next.getValue();
            boolean thAlive = th != null && th.isAlive();
            Long addr = next.getKey();
            String thName = th != null ? th.getName() : "";

            if (log.isInfoEnabled())
                log.info(String.format("Direct IO buffer [address=%d; Thread=%s; alive=%s]", addr, thName, thAlive));

            if (!thAlive) {
                if (addr != null)
                    AlignedBuffers.free(addr);
            }
            else {
                U.warn(log, "Can't free buffer for alive thread: " + thName);
                if (remained == null) {
                    remained = new ArrayList<>(buf.size());
                }
                remained.add(next);
            }
        }
        return remained;
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStart() throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStop(boolean cancel) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Nullable @Override public Serializable provideDiscoveryData(UUID nodeId) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void receiveDiscoveryData(UUID nodeId, Serializable data) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void validateNewNode(ClusterNode node) throws PluginValidationException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object createComponent(PluginContext ctx, Class cls) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public IgnitePlugin plugin() {
        return new LinuxNativeIoPlugin();
    }

    /**
     * @param ignite Ignite starting up.
     * @return Managed aligned buffers and its associated threads. This collection is used to free buffers. May return
     * {@code null}.
     */
    @Nullable private ConcurrentHashMap8<Long, Thread> setupDirect(IgniteEx ignite) {
        GridCacheSharedContext<Object, Object> cacheCtx = ignite.context().cache().context();
        IgnitePageStoreManager ignitePageStoreMgr = cacheCtx.pageStore();

        if (ignitePageStoreMgr == null)
            return null;

        if (!(ignitePageStoreMgr instanceof FilePageStoreManager))
            return null;

        final FilePageStoreManager pageStore = (FilePageStoreManager)ignitePageStoreMgr;
        FileIOFactory backupIoFactory = pageStore.getPageStoreFileIoFactory();

        final AlignedBuffersDirectFileIOFactory factory = new AlignedBuffersDirectFileIOFactory(
            ignite.log(),
            pageStore.workDir(),
            pageStore.pageSize(),
            backupIoFactory);

        if (!factory.isDirectIoAvailable())
            return null;

        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)cacheCtx.database();

        db.setThreadBuf(new ThreadLocal<ByteBuffer>() {
            /** {@inheritDoc} */
            @Override protected ByteBuffer initialValue() {
                return factory.createManagedBuffer(pageStore.pageSize());
            }
        });

        pageStore.setPageStoreFileIOFactories(factory, backupIoFactory);

        return factory.managedAlignedBuffers();
    }
}
