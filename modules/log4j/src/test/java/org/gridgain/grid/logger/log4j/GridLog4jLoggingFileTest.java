/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.logger.log4j;

import junit.framework.*;
import org.apache.ignite.*;
import org.apache.ignite.logger.log4j.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;
import java.io.*;

/**
 * Grid Log4j SPI test.
 */
@GridCommonTest(group = "Logger")
public class GridLog4jLoggingFileTest extends TestCase {
    /** */
    private IgniteLogger log;

    /** {@inheritDoc} */
    @Override protected void setUp() throws Exception {
        File xml = GridTestUtils.resolveGridGainPath("modules/core/src/test/config/log4j-test.xml");

        assert xml != null;
        assert xml.exists() == true;

        log = new IgniteLog4jLogger(xml).getLogger(getClass());
    }

    /**
     * Tests log4j logging SPI.
     */
    public void testLog() {
        assert log.isDebugEnabled() == true;
        assert log.isInfoEnabled() == true;

        log.debug("This is 'debug' message.");
        log.info("This is 'info' message.");
        log.warning("This is 'warning' message.");
        log.warning("This is 'warning' message.", new Exception("It's a test warning exception"));
        log.error("This is 'error' message.");
        log.error("This is 'error' message.", new Exception("It's a test error exception"));
    }
}
