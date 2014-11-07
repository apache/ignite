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
import org.gridgain.grid.resources.*;
import org.gridgain.grid.service.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;

/**
 * Tests for injected service.
 */
public class GridServiceInjectionSelfTest extends GridCommonAbstractTest implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Service name. */
    private static final String SERVICE_NAME1 = "testService1";

    /** Service name. */
    private static final String SERVICE_NAME2 = "testService2";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);
        startGrid(1);

        grid(0).services().deployNodeSingleton(SERVICE_NAME1, new DummyServiceImpl()).get();
        grid(0).forLocal().services().deployClusterSingleton(SERVICE_NAME2, new DummyServiceImpl()).get();

        assertEquals(2, grid(0).nodes().size());
        assertEquals(2, grid(1).nodes().size());
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
            @GridServiceResource(serviceName = SERVICE_NAME1)
            private DummyService svc;

            @Override public Object call() throws Exception {
                assertNotNull(svc);
                assertTrue(svc instanceof GridService);

                svc.noop();

                return null;
            }
        }).get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testClosureFieldProxy() throws Exception {
        grid(0).forRemotes().compute().call(new GridCallable<Object>() {
            @GridServiceResource(serviceName = SERVICE_NAME2, proxyInterface = DummyService.class)
            private DummyService svc;

            @Override public Object call() throws Exception {
                assertNotNull(svc);

                // Ensure proxy instance.
                assertFalse(svc instanceof GridService);

                svc.noop();

                return null;
            }
        }).get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testClosureFieldLocalProxy() throws Exception {
        grid(0).forRemotes().compute().call(new GridCallable<Object>() {
            @GridServiceResource(serviceName = SERVICE_NAME1, proxyInterface = DummyService.class)
            private DummyService svc;

            @Override public Object call() throws Exception {
                assertNotNull(svc);

                // Ensure proxy instance.
                assertTrue(svc instanceof GridService);

                svc.noop();

                return null;
            }
        }).get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testClosureFieldWithIncorrectType() throws Exception {
        try {
            grid(0).compute().call(new GridCallable<Object>() {
                @GridServiceResource(serviceName = SERVICE_NAME1)
                private String svcName;

                @Override public Object call() throws Exception {
                    fail();

                    return null;
                }
            }).get();

            fail();
        }
        catch (GridException e) {
            assertTrue(e.getMessage().startsWith("Remote job threw user exception"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testClosureMethod() throws Exception {
        grid(0).compute().call(new GridCallable<Object>() {
            private DummyService svc;

            @GridServiceResource(serviceName = SERVICE_NAME1)
            private void service(DummyService svc) {
                assertNotNull(svc);

                assertTrue(svc instanceof GridService);

                this.svc = svc;
            }

            @Override public Object call() throws Exception {
                svc.noop();

                return null;
            }
        }).get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testClosureMethodProxy() throws Exception {
        grid(0).forRemotes().compute().call(new GridCallable<Object>() {
            private DummyService svc;

            @GridServiceResource(serviceName = SERVICE_NAME2, proxyInterface = DummyService.class)
            private void service(DummyService svc) {
                assertNotNull(svc);

                // Ensure proxy instance.
                assertFalse(svc instanceof GridService);

                this.svc = svc;
            }

            @Override public Object call() throws Exception {
                svc.noop();

                return null;
            }
        }).get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testClosureMethodLocalProxy() throws Exception {
        grid(0).forRemotes().compute().call(new GridCallable<Object>() {
            private DummyService svc;

            @GridServiceResource(serviceName = SERVICE_NAME1, proxyInterface = DummyService.class)
            private void service(DummyService svc) {
                assertNotNull(svc);

                // Ensure proxy instance.
                assertTrue(svc instanceof GridService);

                this.svc = svc;
            }

            @Override public Object call() throws Exception {
                svc.noop();

                return null;
            }
        }).get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testClosureMethodWithIncorrectType() throws Exception {
        try {
            grid(0).compute().call(new GridCallable<Object>() {
                @GridServiceResource(serviceName = SERVICE_NAME1)
                private void service(String svcs) {
                    fail();
                }

                @Override public Object call() throws Exception {
                    return null;
                }
            }).get();

            fail();
        }
        catch (GridException e) {
            assertTrue(e.getMessage().startsWith("Remote job threw user exception"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testClosureFieldWithNonExistentService() throws Exception {
        grid(0).compute().call(new GridCallable<Object>() {
            @GridServiceResource(serviceName = "nonExistentService")
            private DummyService svc;

            @Override public Object call() throws Exception {
                assertNull(svc);

                return null;
            }
        }).get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testClosureMethodWithNonExistentService() throws Exception {
        grid(0).compute().call(new GridCallable<Object>() {
            @GridServiceResource(serviceName = "nonExistentService")
            private void service(DummyService svc) {
                assertNull(svc);
            }

            @Override public Object call() throws Exception {
                return null;
            }
        }).get();
    }

    /**
     * Dummy Service.
     */
    public interface DummyService {
        public void noop();
    }

    /**
     * No-op test service.
     */
    public static class DummyServiceImpl implements DummyService, GridService {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public void noop() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void cancel(GridServiceContext ctx) {
            System.out.println("Cancelling service: " + ctx.name());
        }

        /** {@inheritDoc} */
        @Override public void init(GridServiceContext ctx) throws Exception {
            System.out.println("Initializing service: " + ctx.name());
        }

        /** {@inheritDoc} */
        @Override public void execute(GridServiceContext ctx) {
            System.out.println("Executing service: " + ctx.name());
        }
    }
}
