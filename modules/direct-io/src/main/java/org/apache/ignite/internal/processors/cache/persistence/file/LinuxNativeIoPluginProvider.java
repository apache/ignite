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

import java.io.FileDescriptor;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.CachePluginContext;
import org.apache.ignite.plugin.CachePluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.plugin.PluginValidationException;
import org.jetbrains.annotations.Nullable;

/**
 * Plugin provider for setting up {@link IgniteNativeIoLib}.
 */
public class LinuxNativeIoPluginProvider implements PluginProvider {
    /** Managed buffers map from address to thread requested buffer. */
    @Nullable private ConcurrentHashMap<Long, Thread> managedBuffers;

    /** Logger. */
    private IgniteLogger log;

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
    @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public CachePluginProvider createCacheProvider(CachePluginContext ctx) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void start(PluginContext ctx) {
        final Ignite ignite = ctx.grid();

        log = ignite.log();
        managedBuffers = setupDirect((IgniteEx)ignite);
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        freeDirectBuffers();
    }

    /**
     * Free direct thread local buffer allocated for Direct IO user's threads.
     */
    private void freeDirectBuffers() {
        ConcurrentHashMap<Long, Thread> buffers = managedBuffers;

        if (buffers == null)
            return;

        managedBuffers = null;

        if (log.isDebugEnabled())
            log.debug("Direct IO buffers to be freed: " + buffers.size());

        for (Map.Entry<Long, Thread> next : buffers.entrySet()) {
            Thread th = next.getValue();
            Long addr = next.getKey();

            if (log.isDebugEnabled())
                log.debug(String.format("Free Direct IO buffer [address=%d; Thread=%s; alive=%s]",
                    addr, th != null ? th.getName() : "", th != null && th.isAlive()));

            AlignedBuffers.free(addr);
        }

        buffers.clear();
    }

    /** {@inheritDoc} */
    @Override public void onIgniteStart() {
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
    @Nullable private ConcurrentHashMap<Long, Thread> setupDirect(IgniteEx ignite) {
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

        final IgniteWriteAheadLogManager walMgr = cacheCtx.wal();

        if (walMgr != null && walMgr instanceof FileWriteAheadLogManager && IgniteNativeIoLib.isJnaAvailable()) {
            ((FileWriteAheadLogManager)walMgr).setCreateWalFileListener(new IgniteInClosure<FileIO>() {
                @Override public void apply(FileIO fileIO) {
                    adviceFileDontNeed(fileIO, ((FileWriteAheadLogManager)walMgr).maxWalSegmentSize());
                }
            });
        }

        if (!factory.isDirectIoAvailable())
            return null;

        GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)cacheCtx.database();

        db.setThreadBuf(new ThreadLocal<ByteBuffer>() {
            @Override protected ByteBuffer initialValue() {
                return factory.createManagedBuffer(pageStore.pageSize());
            }
        });

        pageStore.setPageStoreFileIOFactories(factory, backupIoFactory);

        return factory.managedAlignedBuffers();
    }

    /**
     * Apply advice: The specified data will not be accessed in the near future.
     *
     * Useful for WAL segments to indicate file content won't be loaded.
     *
     * @param fileIO file to advice.
     * @param size expected size of file.
     */
    private void adviceFileDontNeed(FileIO fileIO, long size) {
        try {
            if (fileIO instanceof RandomAccessFileIO) {
                RandomAccessFileIO chIo = (RandomAccessFileIO)fileIO;

                FileChannel ch = U.field(chIo, "ch");

                FileDescriptor fd = U.field(ch, "fd");

                int fdVal = U.field(fd, "fd");

                int retVal = IgniteNativeIoLib.posix_fadvise(fdVal, 0, size, IgniteNativeIoLib.POSIX_FADV_DONTNEED);

                if (retVal != 0) {
                    U.warn(log, "Unable to apply fadvice on WAL file descriptor [fd=" + fdVal + "]:" +
                        IgniteNativeIoLib.strerror(retVal));
                }
            }
        }
        catch (Exception e) {
            U.warn(log, "Unable to advice on WAL file descriptor: [" + e.getMessage() + "]", e);
        }
    }
}
