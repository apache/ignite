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
import org.gridgain.testframework.junits.common.*;

/**
 * Log4j not initialized test.
 */
@GridCommonTest(group = "Logger")
public class GridLog4jNotInitializedTest extends TestCase {
    /** */
    public void testLogInitialize() {
        IgniteLogger log = new IgniteLog4jLogger().getLogger(GridLog4jNotInitializedTest.class);

        if (log.isDebugEnabled())
            log.debug("This is 'debug' message.");
        else
            System.out.println("DEBUG level is not enabled.");

        if (log.isInfoEnabled())
            log.info("This is 'info' message.");
        else
            System.out.println("INFO level is not enabled.");

        log.warning("This is 'warning' message.");
        log.error("This is 'error' message.");
    }
}
