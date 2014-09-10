/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.resource;

import org.gridgain.grid.lang.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.service.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;

/**
 * Tests for injected service.
 */
public class GridServiceInjectionSelfTest extends GridCommonAbstractTest implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Service name. */
    private static final String SERVICE_NAME = "testService";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);

        startGrid(1).services().deployNodeSingleton(SERVICE_NAME, new DummyService()).get();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testClosureField() throws Exception {
        grid(0).compute().call(new GridCallable<Object>() {
            @GridServiceResource(serviceName = "testService")
            private Collection<DummyService> srvcs;

            @Override public Object call() throws Exception {
                assertNotNull(srvcs);

                assertEquals(1, srvcs.size());

                assertNotNull(srvcs.iterator().next());

                return null;
            }
        }).get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testClosureMethod() throws Exception {
        grid(0).compute().call(new GridCallable<Object>() {
            @GridServiceResource(serviceName = "testService")
            private void service(Collection<DummyService> srvcs) {
                assertNotNull(srvcs);

                assertEquals(1, srvcs.size());

                assertNotNull(srvcs.iterator().next());
            }

            @Override public Object call() throws Exception {
                return null;
            }
        }).get();
    }

    /**
     * No-op test service.
     */
    private static class DummyService implements GridService {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public void cancel(GridServiceContext ctx) {
            System.out.println("Cancelling service: " + ctx.name());
        }

        /** {@inheritDoc} */
        @Override public void execute(GridServiceContext ctx) {
            System.out.println("Executing service: " + ctx.name());
        }
    }
}
