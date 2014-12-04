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
import org.apache.log4j.*;
import org.gridgain.testframework.junits.common.*;

/**
 * Log4j initialized test.
 */
@GridCommonTest(group = "Logger")
public class GridLog4jInitializedTest extends TestCase {

    /**
     * @throws Exception If failed.
     */
    @Override protected void setUp() throws Exception {
        BasicConfigurator.configure();
    }

    /** */
    public void testLogInitialize() {
        IgniteLogger log = new IgniteLog4jLogger();

        assert log.isInfoEnabled() == true;

        if (log.isDebugEnabled())
            log.debug("This is 'debug' message.");

        log.info("This is 'info' message.");
        log.warning("This is 'warning' message.");
        log.warning("This is 'warning' message.", new Exception("It's a test warning exception"));
        log.error("This is 'error' message.");
        log.error("This is 'error' message.", new Exception("It's a test error exception"));

        assert log.getLogger(GridLog4jInitializedTest.class.getName()) instanceof IgniteLog4jLogger;
    }
}
