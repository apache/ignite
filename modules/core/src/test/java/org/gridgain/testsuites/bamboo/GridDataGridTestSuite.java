/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testsuites.bamboo;

import junit.framework.*;
import org.gridgain.grid.cache.affinity.fair.*;
import org.gridgain.grid.cache.hibernate.*;
import org.gridgain.grid.cache.store.*;
import org.gridgain.grid.cache.store.hibernate.*;
import org.gridgain.grid.cache.store.jdbc.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.atomic.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.kernal.processors.cache.distributed.replicated.*;
import org.gridgain.grid.kernal.processors.cache.distributed.replicated.preloader.*;
import org.gridgain.grid.kernal.processors.cache.local.*;
import org.gridgain.grid.kernal.processors.dataload.*;
import org.gridgain.grid.kernal.websession.*;
import org.gridgain.testsuites.*;

/**
 * Test suite.
 */
public class GridDataGridTestSuite extends TestSuite {
    /**
     * @return GridGain TeamCity in-memory data grid test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain In-Memory Data Grid Test Suite");

        // Web sessions.
        suite.addTest(GridWebSessionSelfTestSuite.suite());

        return suite;
    }
}
