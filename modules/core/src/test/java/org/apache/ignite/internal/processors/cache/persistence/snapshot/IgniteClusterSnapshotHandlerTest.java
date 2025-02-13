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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.PluginConfiguration;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Snapshot custom handlers test.
 */
public class IgniteClusterSnapshotHandlerTest extends IgniteClusterSnapshotRestoreBaseTest {
    /** Custom snapshot handlers. */
    private final List<SnapshotHandler<?>> handlers = new ArrayList<>();

    /** Extensions plugin provider. */
    private final PluginProvider<PluginConfiguration> pluginProvider = new AbstractTestPluginProvider() {
        @Override public String name() {
            return "SnapshotVerifier";
        }

        @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
            for (SnapshotHandler<?> hnd : handlers)
                registry.registerExtension(SnapshotHandler.class, hnd);
        }
    };

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setPluginProviders(pluginProvider);
    }

    /** {@inheritDoc} */
    @Override protected Function<Integer, Object> valueBuilder() {
        return Integer::new;
    }

    /**
     * Test the basic snapshot metadata consistency using handlers.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testClusterSnapshotHandlers() throws Exception {
        String expMsg = "Inconsistent data";

        AtomicReference<UUID> reqIdRef = new AtomicReference<>();

        handlers.add(new SnapshotHandler<UUID>() {
            @Override public SnapshotHandlerType type() {
                return SnapshotHandlerType.CREATE;
            }

            @Override public UUID invoke(SnapshotHandlerContext ctx) {
                return ctx.metadata().requestId();
            }

            @Override public void complete(String name,
                Collection<SnapshotHandlerResult<UUID>> results) throws IgniteCheckedException {
                for (SnapshotHandlerResult<UUID> res : results) {
                    if (!reqIdRef.compareAndSet(null, res.data()) && !reqIdRef.get().equals(res.data()))
                        throw new IgniteCheckedException("The request ID must be the same on all nodes.");
                }
            }
        });

        handlers.add(new SnapshotHandler<UUID>() {
            @Override public SnapshotHandlerType type() {
                return SnapshotHandlerType.RESTORE;
            }

            @Override public UUID invoke(SnapshotHandlerContext ctx) {
                return ctx.metadata().requestId();
            }

            @Override public void complete(String name,
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
     * @param newReqId Request ID that will be stored on all nodes.
     * @throws Exception If failed.
     */
    private void changeMetadataRequestIdOnDisk(UUID newReqId) throws Exception {
        for (Ignite grid : G.allGrids()) {
            IgniteSnapshotManager snpMgr = ((IgniteEx)grid).context().cache().context().snapshotMgr();
            SnapshotFileTree sft = snapshotFileTree((IgniteEx)grid, SNAPSHOT_NAME);

            SnapshotMetadata metadata = snpMgr.readSnapshotMetadata(sft.meta());

            try (OutputStream out = new BufferedOutputStream(new FileOutputStream(sft.meta()))) {
                GridTestUtils.setFieldValue(metadata, "rqId", newReqId);

                U.marshal(((IgniteEx)grid).context().marshallerContext().jdkMarshaller(), metadata, out);
            }
        }
    }

    /**
     * Test for failures of different types of handlers.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testClusterSnapshotHandlerFailure() throws Exception {
        String expMsg = "Test verification exception message.";

        AtomicBoolean failCreateFlag = new AtomicBoolean(true);
        AtomicBoolean failRestoreFlag = new AtomicBoolean(true);

        handlers.add(new SnapshotHandler<Void>() {
            @Override public SnapshotHandlerType type() {
                return SnapshotHandlerType.CREATE;
            }

            @Override public Void invoke(SnapshotHandlerContext ctx) throws IgniteCheckedException {
                if (failCreateFlag.get())
                    throw new IgniteCheckedException(expMsg);

                return null;
            }
        });

        handlers.add(new SnapshotHandler<Void>() {
            @Override public SnapshotHandlerType type() {
                return SnapshotHandlerType.RESTORE;
            }

            @Override public Void invoke(SnapshotHandlerContext ctx) throws IgniteCheckedException {
                if (failRestoreFlag.get())
                    throw new IgniteCheckedException(expMsg);

                return null;
            }
        });

        IgniteEx ignite = startGridsWithCache(2, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg);

        IgniteFuture<Void> fut = snp(ignite).createSnapshot(SNAPSHOT_NAME, null, false, onlyPrimary);

        GridTestUtils.assertThrowsAnyCause(log, () -> fut.get(TIMEOUT), IgniteCheckedException.class, expMsg);

        failCreateFlag.set(false);

        createAndCheckSnapshot(ignite, SNAPSHOT_NAME, null, TIMEOUT);

        ignite.cache(DEFAULT_CACHE_NAME).destroy();

        awaitPartitionMapExchange();

        IgniteFuture<Void> fut0 = ignite.snapshot().restoreSnapshot(SNAPSHOT_NAME, null);

        GridTestUtils.assertThrowsAnyCause(log, () -> fut0.get(TIMEOUT), IgniteCheckedException.class, expMsg);

        failRestoreFlag.set(false);

        ignite.snapshot().restoreSnapshot(SNAPSHOT_NAME, null).get(TIMEOUT);

        assertCacheKeys(ignite.cache(DEFAULT_CACHE_NAME), CACHE_KEYS_RANGE);
    }

    /**
     * Test ensures that the snapshot operation is aborted if different handlers are loaded on different nodes.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testClusterSnapshotHandlerConfigurationMismatch() throws Exception {
        SnapshotHandler<Void> defHnd = new SnapshotHandler<Void>() {
            @Override public SnapshotHandlerType type() {
                return SnapshotHandlerType.CREATE;
            }

            @Override public Void invoke(SnapshotHandlerContext ctx) {
                return null;
            }
        };

        handlers.add(defHnd);

        startGridsWithCache(1, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg);

        handlers.clear();

        startGrid(1);

        resetBaselineTopology();

        // Case 1: handler is loaded on only one of the two nodes.
        IgniteFuture<Void> fut0 = snp(grid(0)).createSnapshot(SNAPSHOT_NAME, null, false, onlyPrimary);

        UUID nodeId1 = grid(1).localNode().id();

        GridTestUtils.assertThrowsAnyCause(log, () -> fut0.get(TIMEOUT), IgniteCheckedException.class,
            "handler is missing on the remote node(s). The current operation will be aborted [missing=[" + nodeId1 + "]]");

        stopGrid(1);

        // Case 2: different handlers are loaded on different nodes.
        handlers.add(new SnapshotHandler<Void>() {
            @Override public SnapshotHandlerType type() {
                return SnapshotHandlerType.CREATE;
            }

            @Nullable @Override public Void invoke(SnapshotHandlerContext ctx) {
                return null;
            }
        });

        startGrid(1);

        IgniteFuture<Void> fut1 = snp(grid(0)).createSnapshot(SNAPSHOT_NAME, null, false, onlyPrimary);

        GridTestUtils.assertThrowsAnyCause(log, () -> fut1.get(TIMEOUT), IgniteCheckedException.class,
            "Snapshot handlers configuration mismatch (number of local snapshot handlers differs from the remote one)");

        stopGrid(1);

        // Case 3: one handler is loaded on two nodes, the other only on one.
        handlers.add(defHnd);

        startGrid(1);

        IgniteFuture<Void> fut2 = snp(grid(0)).createSnapshot(SNAPSHOT_NAME, null, false, onlyPrimary);

        GridTestUtils.assertThrowsAnyCause(log, () -> fut2.get(TIMEOUT), IgniteCheckedException.class,
            "Snapshot handlers configuration mismatch (number of local snapshot handlers differs from the remote one)");

        stopGrid(1);

        // Make sure the operation was successful with the same configuration.
        handlers.clear();
        handlers.add(defHnd);

        startGrid(1);

        createAndCheckSnapshot(grid(1), SNAPSHOT_NAME, null, TIMEOUT);
    }

    /**
     * Test ensures that the snapshot creation is aborted if node exits while the {@link
     * SnapshotHandler#complete(String, Collection)} method is executed.
     *
     * @throws Exception If fails.
     */
    @Test
    public void testCrdChangeDuringHandlerCompleteOnSnapshotCreate() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        handlers.add(new SnapshotHandler<Void>() {
            @Override public SnapshotHandlerType type() {
                return SnapshotHandlerType.CREATE;
            }

            @Override public Void invoke(SnapshotHandlerContext ctx) {
                return null;
            }

            @Override public void complete(String name, Collection<SnapshotHandlerResult<Void>> results)
                throws Exception {
                if (latch.getCount() == 1) {
                    latch.countDown();

                    Thread.sleep(Long.MAX_VALUE);
                }
            }
        });

        startGridsWithCache(2, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg);

        IgniteFuture<Void> fut = snp(grid(1)).createSnapshot(SNAPSHOT_NAME, null, false, onlyPrimary);

        latch.await();

        UUID crdNodeId = grid(0).localNode().id();

        stopGrid(0, true);

        GridTestUtils.assertThrowsAnyCause(log, () -> fut.get(TIMEOUT), ClusterTopologyCheckedException.class,
            "Snapshot operation interrupted, because baseline node left the cluster: " + crdNodeId);

        startGrid(0);
        snp(grid(0)).createSnapshot(SNAPSHOT_NAME, null, false, onlyPrimary);
    }

    /**
     * Test ensures that the snapshot path is set correctly in the handler context.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testHandlerSnapshotLocation() throws Exception {
        String snpName = "snapshot_30052022";
        File snpDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), "ex_snapshots", true);
        String expFullPath = new File(snpDir, snpName).getAbsolutePath();

        SnapshotHandler<Void> createHnd = new SnapshotHandler<Void>() {
            @Override public SnapshotHandlerType type() {
                return SnapshotHandlerType.CREATE;
            }

            @Override public Void invoke(SnapshotHandlerContext ctx) {
                if (!expFullPath.equals(ctx.snapshotDirectory().getAbsolutePath()))
                    throw new IllegalStateException("Expected " + expFullPath + ", actual " + ctx.snapshotDirectory());

                return null;
            }
        };

        SnapshotHandler<Void> restoreHnd = new SnapshotHandler<Void>() {
            @Override public SnapshotHandlerType type() {
                return SnapshotHandlerType.RESTORE;
            }

            @Override public Void invoke(SnapshotHandlerContext ctx) throws Exception {
                createHnd.invoke(ctx);

                return null;
            }
        };

        handlers.add(createHnd);
        handlers.add(restoreHnd);

        try {
            IgniteEx ignite = startGridsWithCache(1, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg);

            IgniteSnapshotManager snpMgr = ignite.context().cache().context().snapshotMgr();

            createAndCheckSnapshot(ignite, snpName, snpDir.getAbsolutePath(), TIMEOUT);

            ignite.destroyCache(DEFAULT_CACHE_NAME);
            awaitPartitionMapExchange();

            snpMgr.restoreSnapshot(snpName, snpDir.getAbsolutePath(), null).get(TIMEOUT);
        }
        finally {
            U.delete(snpDir);
        }
    }

    /**
     * Test ensures that snapshot fails if some files are absent during the check.
     * @see SnapshotPartitionsQuickVerifyHandler
     */
    @Test
    public void testHandlerExceptionFailSnapshot() throws Exception {
        handlers.add(new SnapshotHandler<Void>() {
            @Override public SnapshotHandlerType type() {
                return SnapshotHandlerType.CREATE;
            }

            @Override public Void invoke(SnapshotHandlerContext ctx) {
                // Someone removes snapshot files during creation.
                // In this case snapshot must fail.
                U.delete(ctx.snapshotDirectory());

                return null;
            }
        });

        IgniteEx ignite = startGridsWithCache(1, CACHE_KEYS_RANGE, valueBuilder(), dfltCacheCfg);

        assertThrowsWithCause(
            () -> snp(ignite).createSnapshot("must_fail", null, false, onlyPrimary).get(getTestTimeout()),
            IgniteException.class
        );
    }
}
