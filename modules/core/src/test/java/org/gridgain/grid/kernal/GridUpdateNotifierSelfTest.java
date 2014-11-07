/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.concurrent.*;

/**
 * Update notifier test.
 */
@GridCommonTest(group = "Kernal Self")
public class GridUpdateNotifierSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 30 * 1000;
    }

    /**
     * @throws Exception If test failed.
     */
    public void testEnt() throws Exception {
        testNotifier(true);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testOs() throws Exception {
        testNotifier(false);
    }

    /**
     * @param ent Enterprise flag.
     * @throws Exception If failed.
     */
    private void testNotifier(boolean ent) throws Exception {
        String site = "www.gridgain." + (ent ? "com" : "org");

        GridUpdateNotifier ntf = new GridUpdateNotifier(null, "x.x.x", site, TEST_GATEWAY, false);

        ntf.checkForNewVersion(new SelfExecutor(), log);

        String ver = ntf.latestVersion();

        info("Latest version: " + ver);

        assertNotNull("GridGain latest version has not been detected.", ver);

        ntf.reportStatus(log);
    }

    /**
     * Executor that runs task in current thread.
     */
    private static class SelfExecutor implements Executor {
        /** {@inheritDoc} */
        @Override public void execute(@NotNull Runnable r) {
            r.run();
        }
    }

    /**
     * Test kernal gateway that always return uninitialized user stack trace.
     */
    private static final GridKernalGateway TEST_GATEWAY = new GridKernalGateway() {
        @Override public void lightCheck() throws IllegalStateException {}

        @Override public void readLock() throws IllegalStateException {}

        @Override public void setState(GridKernalState state) {}

        @Override public GridKernalState getState() {
            return null;
        }

        @Override public void readUnlock() {}

        @Override public void writeLock() {}

        @Override public void writeUnlock() {}

        @Override public void addStopListener(Runnable lsnr) {}

        @Override public void removeStopListener(Runnable lsnr) {}

        @Override public String userStackTrace() {
            return null;
        }
    };
}
