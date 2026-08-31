package org.apache.ignite.internal.commandline.snapshot;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotPartitionsVerifyResult;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.util.GridCommandHandlerAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_SNAPSHOT_RESTORE_STARTED;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 *
 */
public class SnapshotStatusReproducerTest extends GridCommandHandlerAbstractTest {
    /** Snapshot check latch. */
    private static CountDownLatch snapshotCheckLatch;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setPluginProviders(new BlockingCheckSnapshotPluginProvider())
            .setIncludeEventTypes(EVT_CLUSTER_SNAPSHOT_RESTORE_STARTED);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        autoConfirmation = false;

        cleanPersistenceDir();

        snapshotCheckLatch = new CountDownLatch(1);

        startGrids(3);

        grid(0).cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cache = grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);

        IntStream.range(0, 2048).forEach(i -> cache.put(i, i));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids(true);

        cleanPersistenceDir();
    }

    /**
     *
     */
    @Test
    public void test() throws Exception {
        injectTestSystemOut();

        IgniteSnapshotManager snapshotMgr = (IgniteSnapshotManager)grid(0).snapshot();

        String snapshotName = "test_snapshot";

        snapshotMgr.createSnapshot(snapshotName).get(getTestTimeout());

        grid(0).destroyCache(DEFAULT_CACHE_NAME);

        CountDownLatch restoreStarterdLatch = new CountDownLatch(1);

        grid(0).events().localListen(
            e -> {
                restoreStarterdLatch.countDown();

                return false;
            },
            EVT_CLUSTER_SNAPSHOT_RESTORE_STARTED
        );

        IgniteFutureImpl<Void> restoreFut = snapshotMgr.restoreSnapshot(snapshotName, null, null, 0, true);

        try {
            restoreStarterdLatch.await(getTestTimeout(), MILLISECONDS);
            assertFalse("Snapshot future has finished", restoreFut.isDone());

            int code = execute("--snapshot", "restore", snapshotName, "--status");

            assertEquals("Unexpected exit code", EXIT_CODE_OK, code);

            assertFalse("Snapshot future has finished", restoreFut.isDone());

            code = execute("--snapshot", "status");

            assertEquals("Unexpected exit code", EXIT_CODE_OK, code);
            assertContains(log, testOut.toString(), "Restore snapshot operation is in progress.");
        }
        finally {
            snapshotCheckLatch.countDown();

            // Wait for future to finish in order to avoid excessive message about task cancellation.
            restoreFut.get();
        }
    }

    /** */
    private static class BlockingCheckSnapshotPluginProvider extends AbstractTestPluginProvider {
        /** {@inheritDoc} */
        @Override public String name() {
            return "BlockingCheckSnapshotPluginProvider";
        }

        /** {@inheritDoc} */
        @Override public <T> @Nullable T createComponent(PluginContext ctx, Class<T> cls) {
            if (IgniteSnapshotManager.class.equals(cls))
                return (T)new BlockingCheckSnapshotManager(((IgniteEx)ctx.grid()).context());

            return null;
        }
    }

    /** Blocks restore proccess. */
    protected static class BlockingCheckSnapshotManager extends IgniteSnapshotManager {
        /** */
        public BlockingCheckSnapshotManager(GridKernalContext ctx) {
            super(ctx);
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture<SnapshotPartitionsVerifyResult> checkSnapshot(
            String name,
            @Nullable String snpPath,
            @Nullable Collection<String> grps,
            boolean includeCustomHandlers,
            int incIdx, boolean check
        ) {
            return super.checkSnapshot(name, snpPath, grps, includeCustomHandlers, incIdx, check)
                .chain(fut -> {
                    try {
                        if(check)
                           ;// snapshotCheckLatch.await(5, MINUTES);
                    }
                    catch (Throwable e) {
                        log.error(X.getFullStackTrace(e));
                    }

                    return fut.result();
                });
        }
    }
}
