/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.logger.log4j;

import junit.framework.*;
import org.gridgain.grid.logger.*;
import org.gridgain.testframework.junits.common.*;

/**
 * Grid Log4j SPI test.
 */
@GridCommonTest(group = "Logger")
public class GridLog4jLoggingPathTest extends TestCase {
    /** */
    private GridLogger log;

    /** {@inheritDoc} */
    @Override protected void setUp() throws Exception {
        log = new GridLog4jLogger("modules/core/src/test/config/log4j-test.xml").getLogger(getClass());
    }

    /**
     * Tests log4j logging SPI.
     */
    public void testLog() {
        assert log.isInfoEnabled() == true;

        if (log.isDebugEnabled())
            log.debug("This is 'debug' message.");

        log.info("This is 'info' message.");
        log.warning("This is 'warning' message.");
        log.warning("This is 'warning' message.", new Exception("It's a test warning exception"));
        log.error("This is 'error' message.");
        log.error("This is 'error' message.", new Exception("It's a test error exception"));
    }
}
