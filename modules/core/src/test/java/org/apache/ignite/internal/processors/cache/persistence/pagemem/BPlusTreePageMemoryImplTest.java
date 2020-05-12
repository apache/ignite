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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.Collections;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.encryption.GridEncryptionManager;
import org.apache.ignite.internal.managers.systemview.GridSystemViewManager;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.processors.cache.CacheDiagnosticManager;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointProgressImpl;
import org.apache.ignite.internal.processors.database.BPlusTreeSelfTest;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.plugin.IgnitePluginProcessor;
import org.apache.ignite.internal.processors.subscription.GridInternalSubscriptionProcessor;
import org.apache.ignite.internal.util.typedef.CIX3;
import org.apache.ignite.lang.IgniteOutClosure;
import org.apache.ignite.spi.encryption.noop.NoopEncryptionSpi;
import org.apache.ignite.spi.metric.noop.NoopMetricExporterSpi;
import org.apache.ignite.spi.systemview.jmx.JmxSystemViewExporterSpi;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.mockito.Mockito;

import static org.apache.ignite.internal.processors.database.DataRegionMetricsSelfTest.NO_OP_METRICS;

/**
 *
 */
public class BPlusTreePageMemoryImplTest extends BPlusTreeSelfTest {
    /** {@inheritDoc} */
    @Override protected PageMemory createPageMemory() throws Exception {
        long[] sizes = new long[CPUS + 1];

        for (int i = 0; i < sizes.length; i++)
            sizes[i] = 1024 * MB / CPUS;

        sizes[CPUS] = 10 * MB;

        DirectMemoryProvider provider = new UnsafeMemoryProvider(log);

        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setEncryptionSpi(new NoopEncryptionSpi());
        cfg.setMetricExporterSpi(new NoopMetricExporterSpi());
        cfg.setSystemViewExporterSpi(new JmxSystemViewExporterSpi());

        GridTestKernalContext cctx = new GridTestKernalContext(log, cfg);

        cctx.add(new IgnitePluginProcessor(cctx, cfg, Collections.emptyList()));
        cctx.add(new GridInternalSubscriptionProcessor(cctx));
        cctx.add(new GridEncryptionManager(cctx));
        cctx.add(new GridMetricManager(cctx));
        cctx.add(new GridSystemViewManager(cctx));

        GridCacheSharedContext<Object, Object> sharedCtx = new GridCacheSharedContext<>(
            cctx,
            null,
            null,
            null,
            new NoOpPageStoreManager(),
            new NoOpWALManager(),
            null,
            new IgniteCacheDatabaseSharedManager(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            new CacheDiagnosticManager()
        );

        IgniteOutClosure<CheckpointProgress> clo = new IgniteOutClosure<CheckpointProgress>() {
            @Override public CheckpointProgress apply() {
                return Mockito.mock(CheckpointProgressImpl.class);
            }
        };

        PageMemory mem = new PageMemoryImpl(
            provider, sizes,
            sharedCtx,
            PAGE_SIZE,
            (fullPageId, byteBuf, tag) -> {
                assert false : "No page replacement should happen during the test";
            },
            new CIX3<Long, FullPageId, PageMemoryEx>() {
                @Override public void applyx(Long aLong, FullPageId fullPageId, PageMemoryEx ex) {
                }
            },
            () -> true,
            new DataRegionMetricsImpl(new DataRegionConfiguration(), cctx.metric(), NO_OP_METRICS),
            PageMemoryImpl.ThrottlingPolicy.DISABLED,
            clo
        );

        mem.start();

        return mem;
    }

    /** {@inheritDoc} */
    @Override protected long acquiredPages() {
        return ((PageMemoryImpl)pageMem).acquiredPages();
    }
}
