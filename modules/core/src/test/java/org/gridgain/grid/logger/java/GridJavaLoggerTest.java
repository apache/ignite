/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.logger.java;

import junit.framework.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

/**
 * Java logger test.
 */
@GridCommonTest(group = "Logger")
public class GridJavaLoggerTest extends TestCase {
    /** */
    @SuppressWarnings({"FieldCanBeLocal"})
    private GridLogger log;

    /** */
    public void testLogInitialize() throws Exception {
        U.setWorkDirectory(null, U.getGridGainHome());

        log = new GridJavaLogger();

        ((GridLoggerNodeIdAware)log).setNodeId(UUID.fromString("00000000-1111-2222-3333-444444444444"));

        if (log.isDebugEnabled())
            log.debug("This is 'debug' message.");

        assert log.isInfoEnabled();

        log.info("This is 'info' message.");
        log.warning("This is 'warning' message.");
        log.warning("This is 'warning' message.", new Exception("It's a test warning exception"));
        log.error("This is 'error' message.");
        log.error("This is 'error' message.", new Exception("It's a test error exception"));

        assert log.getLogger(GridJavaLoggerTest.class.getName()) instanceof GridJavaLogger;

        assert log.fileName() != null;

        // Ensure we don't get pattern, only actual file name is allowed here.
        assert !log.fileName().contains("%");
    }
}
