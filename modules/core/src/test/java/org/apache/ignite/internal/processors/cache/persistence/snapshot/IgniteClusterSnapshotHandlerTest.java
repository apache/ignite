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

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.SNAPSHOT_METAFILE_EXT;

public class IgniteClusterSnapshotHandlerTest extends IgniteClusterSnapshotRestoreBaseTest {
    /** */
    private List<SnapshotHandler<?>> extensions = new ArrayList<>();

    private SnapshotLifecyclePluginProvider testPluginProvider = new SnapshotLifecyclePluginProvider(extensions);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setPluginProviders(testPluginProvider);
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotHandler() throws Exception {
        String expMsg = "Inconsistent data";

        AtomicReference<UUID> reqIdRef = new AtomicReference<>();

        extensions.add(new SnapshotHandler<UUID>() {
            @Override public SnapshotHandlerType type() {
                return SnapshotHandlerType.CREATE;
            }

            @Override public UUID handle(SnapshotHandlerContext ctx) {
                return ctx.metadata().requestId();
            }

            @Override public void reduce(String name,
                Collection<SnapshotHandlerResult<UUID>> results) throws IgniteCheckedException {
                for (SnapshotHandlerResult<UUID> res : results) {
                    if (!reqIdRef.compareAndSet(null, res.data()) && !reqIdRef.get().equals(res.data()))
                        throw new IgniteCheckedException("The request ID must be the same on all nodes.");
                }
            }
        });

        extensions.add(new SnapshotHandler<UUID>() {
            @Override public SnapshotHandlerType type() {
                return SnapshotHandlerType.RESTORE;
            }

            @Override public UUID handle(SnapshotHandlerContext ctx) {
                return ctx.metadata().requestId();
            }

            @Override public void reduce(String name,
                Collection<SnapshotHandlerResult<UUID>> results) throws IgniteCheckedException {
                for (SnapshotHandlerResult<UUID> res : results) {
                    if (!reqIdRef.get().equals(res.data()))
                        throw new IgniteCheckedException(expMsg);
                }
            }
        });

        IgniteEx ignite = startGridsWithSnapshot(2, CACHE_KEYS_RANGE);

        assertNotNull(reqIdRef.get());

        changeMetadataRequestIdOnDisk(UUID.randomUUID());

        IgniteFuture<Void> fut = ignite.snapshot().restoreSnapshot(SNAPSHOT_NAME, null);

        GridTestUtils.assertThrowsAnyCause(log, () -> fut.get(TIMEOUT), IgniteCheckedException.class, expMsg);

        changeMetadataRequestIdOnDisk(reqIdRef.get());

        ignite.snapshot().restoreSnapshot(SNAPSHOT_NAME, null).get(TIMEOUT);

        assertCacheKeys(ignite.cache(DEFAULT_CACHE_NAME), CACHE_KEYS_RANGE);
    }

    /**
     * @param newReqId New request ID.
     * @throws Exception If failed.
     */
    private void changeMetadataRequestIdOnDisk(UUID newReqId) throws Exception {
        for (Ignite grid : G.allGrids()) {
            IgniteSnapshotManager snpMgr = ((IgniteEx)grid).context().cache().context().snapshotMgr();
            String constId = grid.cluster().localNode().consistentId().toString();
            String metaFilename = U.maskForFileName(constId) + SNAPSHOT_METAFILE_EXT;
            SnapshotMetadata meta = snpMgr.readSnapshotMetadata(SNAPSHOT_NAME, constId);
            File smf = new File(snpMgr.snapshotLocalDir(SNAPSHOT_NAME), metaFilename);

            GridTestUtils.setFieldValue(meta, "rqId", newReqId);

            try (OutputStream out = new BufferedOutputStream(new FileOutputStream(smf))) {
                U.marshal(MarshallerUtils.jdkMarshaller(grid.name()), meta, out);
            }
        }
    }

    /** @throws Exception If fails. */
    @Test
    public void testClusterSnapshotHandlerFailure() throws Exception {
        String expMsg = "Test verification exception message.";

        AtomicBoolean failCreateFlag = new AtomicBoolean(true);
        AtomicBoolean failRestoreFlag = new AtomicBoolean(true);

        extensions.add(new SnapshotHandler<Void>() {
            @Override public SnapshotHandlerType type() {
                return SnapshotHandlerType.CREATE;
            }

            @Override public Void handle(SnapshotHandlerContext ctx) throws IgniteCheckedException {
                if (failCreateFlag.get())
                    throw new IgniteCheckedException(expMsg);

                return null;
            }
        });

        extensions.add(new SnapshotHandler<Void>() {
            @Override public SnapshotHandlerType type() {
                return SnapshotHandlerType.RESTORE;
            }

            @Override public Void handle(SnapshotHandlerContext ctx) throws IgniteCheckedException {
                if (failRestoreFlag.get())
                    throw new IgniteCheckedException(expMsg);

                return null;
            }
        });

        IgniteEx ignite = startGridsWithCache(2, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg);

        IgniteFuture<Void> fut = ignite.snapshot().createSnapshot(SNAPSHOT_NAME);

        GridTestUtils.assertThrowsAnyCause(log, () -> fut.get(TIMEOUT), IgniteCheckedException.class, expMsg);

        failCreateFlag.set(false);

        ignite.snapshot().createSnapshot(SNAPSHOT_NAME).get(TIMEOUT);

        ignite.cache(DEFAULT_CACHE_NAME).destroy();

        awaitPartitionMapExchange();

        IgniteFuture<Void> fut0 = ignite.snapshot().restoreSnapshot(SNAPSHOT_NAME, null);

        GridTestUtils.assertThrowsAnyCause(log, () -> fut0.get(TIMEOUT), IgniteCheckedException.class, expMsg);

        failRestoreFlag.set(false);

        ignite.snapshot().restoreSnapshot(SNAPSHOT_NAME, null).get(TIMEOUT);

        assertCacheKeys(ignite.cache(DEFAULT_CACHE_NAME), CACHE_KEYS_RANGE);
    }

    private static class SnapshotLifecyclePluginProvider extends AbstractTestPluginProvider {
        private final List<SnapshotHandler<?>> extensions;

        public SnapshotLifecyclePluginProvider(List<SnapshotHandler<?>> extensions) {
            this.extensions = extensions;
        }

        @Override public String name() {
            return "SnapshotVerifier";
        }

        /** {@inheritDoc} */
        @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
            for (SnapshotHandler<?> hnd : extensions)
                registry.registerExtension(SnapshotHandler.class, hnd);
        }
    }

    /** {@inheritDoc} */
    @Override protected Function<Integer, Object> valueBuilder() {
        return Integer::new;
    }
}
