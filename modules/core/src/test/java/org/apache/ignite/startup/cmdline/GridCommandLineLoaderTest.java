/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.startup.cmdline;

import org.apache.ignite.*;
import org.apache.ignite.startup.cmdline.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;
import java.util.concurrent.*;

import static org.apache.ignite.IgniteState.*;

/**
 * Command line loader test.
 */
@GridCommonTest(group = "Loaders")
public class GridCommandLineLoaderTest extends GridCommonAbstractTest {
    /** */
    private static final String GRID_CFG_PATH = "/modules/core/src/test/config/loaders/grid-cfg.xml";

    /** */
    private final CountDownLatch latch = new CountDownLatch(2);

    /** */
    public GridCommandLineLoaderTest() {
        super(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLoader() throws Exception {
        String path = U.getGridGainHome() + GRID_CFG_PATH;

        info("Loading Grid from configuration file: " + path);

        G.addListener(new GridGainListener() {
            @Override public void onStateChange(String name, IgniteState state) {
                if (state == STARTED) {
                    info("Received started notification from grid: " + name);

                    latch.countDown();

                    G.stop(name, true);
                }
            }
        });

        IgniteCommandLineStartup.main(new String[]{path});
    }
}
