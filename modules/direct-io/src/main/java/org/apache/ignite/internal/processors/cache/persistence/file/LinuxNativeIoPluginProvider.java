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
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
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

public class LinuxNativeIoPluginProvider implements PluginProvider {

    @Nullable private ConcurrentHashMap8<Long, Thread> managedBuffers;
    private IgniteLogger log;

    public LinuxNativeIoPluginProvider() {
        System.err.println();
    }

    @Override public String name() {
        return "Ignite Native I/O Plugin [Direct I/O]";
    }

    @Override public String version() {
        return "";
    }

    @Override public String copyright() {
        return "Copyright(C) Apache Software Foundation";
    }

    @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) throws IgniteCheckedException {
        // No-op.
    }

    @Override public CachePluginProvider createCacheProvider(CachePluginContext ctx) {
        return null;
    }

    @Override public void start(PluginContext ctx) throws IgniteCheckedException {
        Ignite ignite = ctx.grid();
        managedBuffers = setupDirect((IgniteEx)ignite);
        log = ignite.log();
    }

    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        if (managedBuffers != null) {
            log.info("Direct IO buffers to be freed: " + managedBuffers.size());
            for (Map.Entry<Long, Thread> next : managedBuffers.entrySet()) {

                Thread th = next.getValue();
                boolean thAlive = th.isAlive();
                Long addr = next.getKey();
                String thName = th.getName();

                if (log.isInfoEnabled())
                    log.info("Direct IO buffer addr=" + addr + " T:" + thName + " alive " + thAlive);

                if (thAlive)
                    AlignedBuffers.free(addr);
                else
                    U.warn(log, "Can't free buffer for alive thread: " + thName);
            }
        }
    }

    @Override public void onIgniteStart() throws IgniteCheckedException {
        // No-op.
    }

    @Override public void onIgniteStop(boolean cancel) {
        // No-op.
    }

    @Nullable @Override public Serializable provideDiscoveryData(UUID nodeId) {
        return null;
    }

    @Override public void receiveDiscoveryData(UUID nodeId, Serializable data) {
        // No-op.
    }

    @Override public void validateNewNode(ClusterNode node) throws PluginValidationException {
        // No-op.
    }

    @Nullable @Override public Object createComponent(PluginContext ctx, Class cls) {
        return null;
    }

    @Override public IgnitePlugin plugin() {
        return new LinuxNativeIoPlugin();
    }

    @Nullable private ConcurrentHashMap8<Long, Thread> setupDirect(IgniteEx ignite) {
        final GridCacheSharedContext<Object, Object> cacheCtx = ignite.context().cache().context();
        final IgnitePageStoreManager ignitePageStoreManager = cacheCtx.pageStore();

        if (ignitePageStoreManager == null)
            return null;

        if (!(ignitePageStoreManager instanceof FilePageStoreManager))
            return null;

        final FilePageStoreManager pageStore = (FilePageStoreManager)ignitePageStoreManager;

        final FileIOFactory backupIoFactory = pageStore.getPageStoreFileIoFactory();
        final AlignedBuffersDirectFileIOFactory factory = new AlignedBuffersDirectFileIOFactory(
            ignite.log(),
            pageStore.workDir(),
            pageStore.pageSize(),
            backupIoFactory);
        final ConcurrentHashMap8<Long, Thread> buffers = factory.managedAlignedBuffers();
        if (factory.isDirectIoAvailable()) {
            GridCacheDatabaseSharedManager db = (GridCacheDatabaseSharedManager)cacheCtx.database();

            db.setThreadBuf(new ThreadLocal<ByteBuffer>() {
                /** {@inheritDoc} */
                @Override protected ByteBuffer initialValue() {
                    return factory.createManagedBuffer(pageStore.pageSize());
                }
            });

            pageStore.pageStoreFileIoFactory(factory, backupIoFactory);
        }

        return buffers;
    }

}
