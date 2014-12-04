/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.logger.jcl;

import junit.framework.*;
import org.apache.commons.logging.*;
import org.gridgain.grid.logger.*;
import org.gridgain.testframework.junits.common.*;

/**
 * Jcl logger test.
 */
@GridCommonTest(group = "Logger")
public class GridJclLoggerTest extends TestCase {
    /** */
    @SuppressWarnings({"FieldCanBeLocal"})
    private IgniteLogger log;

    /** */
    public void testLogInitialize() {
        log = new GridJclLogger(LogFactory.getLog(GridJclLoggerTest.class.getName()));

        assert log.isInfoEnabled() == true;

        log.info("This is 'info' message.");
        log.warning("This is 'warning' message.");
        log.warning("This is 'warning' message.", new Exception("It's a test warning exception"));
        log.error("This is 'error' message.");
        log.error("This is 'error' message.", new Exception("It's a test error exception"));

        assert log.getLogger(GridJclLoggerTest.class.getName()) instanceof GridJclLogger;
    }
}
