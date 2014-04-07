/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import javax.swing.*;

/**
 * GridGain startup.
 */
@SuppressWarnings({"ProhibitedExceptionDeclared"})
@GridCommonTest(group = "Kernal")
public class GridStartupTest extends GridCommonAbstractTest {
    /** */
    public GridStartupTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return Long.MAX_VALUE;
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartup() throws Exception {
        //resetLog4j("org.gridgain.grid.kernal.processors.cache.distributed.dht.preloader", Level.DEBUG, false, 0);

        //G.start("modules/tests/config/spring-multicache.xml");
        //G.start("examples/config/example-cache.xml");

        G.start();

        // Wait until Ok is pressed.
        JOptionPane.showMessageDialog(
            null,
            new JComponent[] {
                new JLabel("GridGain started."),
                new JLabel(
                    "<html>" +
                        "You can use JMX console at <u>http://localhost:1234</u>" +
                    "</html>"),
                new JLabel("Press OK to stop GridGain.")
            },
            "GridGain Startup JUnit",
            JOptionPane.INFORMATION_MESSAGE
        );

        G.stop(true);
    }
}
