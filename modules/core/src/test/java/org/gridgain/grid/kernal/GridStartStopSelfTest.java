/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;
import static org.gridgain.grid.GridSystemProperties.*;
import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;

/**
 * Checks basic node start/stop operations.
 */
@SuppressWarnings({"CatchGenericClass", "InstanceofCatchParameter"})
@GridCommonTest(group = "Kernal Self")
public class GridStartStopSelfTest extends GridCommonAbstractTest {
    /** */
    public static final int COUNT = 1;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        System.setProperty(GG_OVERRIDE_MCAST_GRP, GridTestUtils.getNextMulticastGroup(GridStartStopSelfTest.class));
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartStop() throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setRestEnabled(false);

        info("Grid start-stop test count: " + COUNT);

        for (int i = 0; i < COUNT; i++) {
            info("Starting grid.");

            try (Ignite g = G.start(cfg)) {
                assert g != null;

                info("Stopping grid " + g.cluster().localNode().id());
            }
        }
    }

    /**
     * TODO: GG-7704
     * @throws Exception If failed.
     */
    public void _testStopWhileInUse() throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setRestEnabled(false);

        cfg.setGridName(getTestGridName(0));

        GridCacheConfiguration cc = new GridCacheConfiguration();

        cc.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheConfiguration(cc);

        final Ignite g0 = G.start(cfg);

        cfg = new IgniteConfiguration();

        cfg.setGridName(getTestGridName(1));

        cc = new GridCacheConfiguration();

        cc.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheConfiguration(cc);

        final CountDownLatch latch = new CountDownLatch(1);

        Ignite g1 = G.start(cfg);

        Thread stopper = new Thread(new Runnable() {
            @Override public void run() {
                try {
                    try (GridCacheTx ignored = g0.cache(null).txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        g0.cache(null).get(1);

                        latch.countDown();

                        Thread.sleep(500);

                        info("Before stop.");

                        G.stop(getTestGridName(1), true);
                    }
                }
                catch (Exception e) {
                    error("Error.", e);
                }
            }
        });

        stopper.start();

        assert latch.await(1, SECONDS);

        info("Before remove.");

        g1.cache(null).remove(1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStoppedState() throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setRestEnabled(false);

        Ignite ignite = G.start(cfg);

        assert ignite != null;

        G.stop(ignite.name(), true);

        try {
            ignite.cluster().localNode();
        }
        catch (Exception e) {
            assert e instanceof IllegalStateException : "Wrong exception type.";
        }

        try {
            ignite.cluster().nodes();

            assert false;
        }
        catch (Exception e) {
            assert e instanceof IllegalStateException : "Wrong exception type.";
        }

        try {
            ignite.cluster().forRemotes();

            assert false;
        }
        catch (Exception e) {
            assert e instanceof IllegalStateException : "Wrong exception type.";
        }

        try {
            ignite.compute().localTasks();

            assert false;
        }
        catch (Exception e) {
            assert e instanceof IllegalStateException : "Wrong exception type.";
        }
    }
}
