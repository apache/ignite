/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.testframework.junits.common.*;

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
    public void testPlatformEnt() throws Exception {
        testNotifier("platform", true);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testPlatformOs() throws Exception {
        testNotifier("platform", false);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDataGridEnt() throws Exception {
        testNotifier("datagrid", true);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testDataGridOs() throws Exception {
        testNotifier("datagrid", false);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testHadoopEnt() throws Exception {
        testNotifier("hadoop", true);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testHadoopOs() throws Exception {
        testNotifier("hadoop", false);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testStreamingEnt() throws Exception {
        testNotifier("streaming", true);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testStreamingOs() throws Exception {
        testNotifier("streaming", false);
    }

    /**
     * @param edition Edition.
     * @param ent Enterprise flag.
     * @throws Exception If failed.
     */
    private void testNotifier(String edition, boolean ent) throws Exception {
        String site = "www.gridgain." + (ent ? "com" : "org");

        GridUpdateNotifier ntf = new GridUpdateNotifier(null, edition, "x.x.x", site, false);

        ntf.checkForNewVersion(new SelfExecutor(), log);

        String ver = ntf.latestVersion();

        info("Latest " + edition + " version: " + ver);

        assertNotNull("GridGain latest version has not been detected.", ver);

        ntf.reportStatus(log);
    }

    /**
     * Executor that runs task in current thread.
     */
    private static class SelfExecutor implements Executor {
        /** {@inheritDoc} */
        @Override public void execute(Runnable r) {
            r.run();
        }
    }
}
