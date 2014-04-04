/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

/**
 * Grid log throttle test. To verify correctness, you need to run this test
 * and check that all messages that should be logged are indeed logged and
 * all messages that should be omitted are indeed omitted.
 */
@GridCommonTest(group = "Utils")
public class GridLogThrottleTest extends GridCommonAbstractTest {
    /** Constructor. */
    public GridLogThrottleTest() {
        super(false);
    }

    /**
     * Tests throttle.
     *
     * @throws Exception If any error occurs.
     */
    public void testThrottle() throws Exception {
        LT.throttleTimeout(1000);

        // LOGGED.
        LT.error(log, new RuntimeException("Test exception 1."), "Test");

        // OMITTED.
        LT.error(log, new RuntimeException("Test exception 1."), "Test");

        // OMITTED.
        LT.error(log, new RuntimeException("Test exception 1."), "Test1");

        // LOGGED.
        LT.error(log, new RuntimeException("Test exception 2."), "Test");

        // OMITTED.
        LT.warn(log, new RuntimeException("Test exception 1."), "Test");

        // OMITTED.
        LT.warn(log, new RuntimeException("Test exception 2."), "Test1");

        // OMITTED.
        LT.warn(log, new RuntimeException("Test exception 2."), "Test3");

        // LOGGED.
        LT.error(log, null, "Test - without throwable.");

        // OMITTED.
        LT.error(log, null, "Test - without throwable.");

        // LOGGED.
        LT.warn(log, null, "Test - without throwable1.");

        // OMITTED.
        LT.warn(log, null, "Test - without throwable1.");

        Thread.sleep(LT.throttleTimeout());

        info("Slept for throttle timeout: " + LT.throttleTimeout());

        // LOGGED.
        LT.error(log, new RuntimeException("Test exception 1."), "Test");

        // OMITTED.
        LT.error(log, new RuntimeException("Test exception 1."), "Test");

        // OMITTED.
        LT.error(log, new RuntimeException("Test exception 1."), "Test1");

        // LOGGED.
        LT.error(log, new RuntimeException("Test exception 2."), "Test");

        // OMITTED.
        LT.warn(log, new RuntimeException("Test exception 1."), "Test");

        // OMITTED.
        LT.warn(log, new RuntimeException("Test exception 2."), "Test1");

        // OMITTED.
        LT.warn(log, new RuntimeException("Test exception 2."), "Test3");

        Thread.sleep(LT.throttleTimeout());

        info("Slept for throttle timeout: " + LT.throttleTimeout());

        //LOGGED.
        LT.info(log(), "Test info message.");

        //OMMITED.
        LT.info(log(), "Test info message.");

        //OMMITED.
        LT.info(log(), "Test info message.");

        //OMMITED.
        LT.info(log(), "Test info message.");

        //OMMITED.
        LT.info(log(), "Test info message.");

        //OMMITED.
        LT.info(log(), "Test info message.");
    }
}
