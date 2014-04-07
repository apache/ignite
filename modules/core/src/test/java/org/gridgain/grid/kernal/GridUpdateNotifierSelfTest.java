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
    public void testNotifier() throws Exception {
        final GridUpdateNotifier ntf = new GridUpdateNotifier(null, false);

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
        @Override public void execute(Runnable r) {
            r.run();
        }
    }
}
