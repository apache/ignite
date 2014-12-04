/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.resource;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.junit.*;

/**
 * Top-level closure class.
 */
public class TestClosure implements IgniteClosure<Object, Object> {
    /** */
    @IgniteInstanceResource
    private Ignite ignite;

    /** */
    @IgniteLoggerResource
    private IgniteLogger log;

    @Override public Object apply(Object o) {
        Assert.assertNotNull(ignite);
        Assert.assertNotNull(log);

        log.info("Closure is running with grid: " + ignite);

        return null;
    }
}
