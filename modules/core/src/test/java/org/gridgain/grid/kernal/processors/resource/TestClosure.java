/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.resource;

import org.gridgain.grid.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.junit.*;

/**
 * Top-level closure class.
 */
public class TestClosure implements GridClosure<Object, Object> {
    /** */
    @GridInstanceResource
    private Grid grid;

    /** */
    @GridLoggerResource
    private GridLogger log;

    @Override public Object apply(Object o) {
        Assert.assertNotNull(grid);
        Assert.assertNotNull(log);

        log.info("Closure is running with grid: " + grid);

        return null;
    }
}
