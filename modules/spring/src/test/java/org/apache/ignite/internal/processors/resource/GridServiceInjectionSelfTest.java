/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.resource;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.managed.*;
import org.apache.ignite.resources.*;
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

        grid(0).managed().deployNodeSingleton(SERVICE_NAME1, new DummyServiceImpl());
        grid(0).managed(grid(0).cluster().forLocal()).deployClusterSingleton(SERVICE_NAME2, new DummyServiceImpl());

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
        grid(0).compute().call(new IgniteCallable<Object>() {
            @IgniteServiceResource(serviceName = SERVICE_NAME1)
            private DummyService svc;

            @Override public Object call() throws Exception {
                assertNotNull(svc);
                assertTrue(svc instanceof ManagedService);

                svc.noop();

                return null;
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testClosureFieldProxy() throws Exception {
        grid(0).compute(grid(0).cluster().forRemotes()).call(new IgniteCallable<Object>() {
            @IgniteServiceResource(serviceName = SERVICE_NAME2, proxyInterface = DummyService.class)
            private DummyService svc;

            @Override public Object call() throws Exception {
                assertNotNull(svc);

                // Ensure proxy instance.
                assertFalse(svc instanceof ManagedService);

                svc.noop();

                return null;
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testClosureFieldLocalProxy() throws Exception {
        grid(0).compute(grid(0).cluster().forRemotes()).call(new IgniteCallable<Object>() {
            @IgniteServiceResource(serviceName = SERVICE_NAME1, proxyInterface = DummyService.class)
            private DummyService svc;

            @Override public Object call() throws Exception {
                assertNotNull(svc);

                // Ensure proxy instance.
                assertTrue(svc instanceof ManagedService);

                svc.noop();

                return null;
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testClosureFieldWithIncorrectType() throws Exception {
        try {
            grid(0).compute().call(new IgniteCallable<Object>() {
                @IgniteServiceResource(serviceName = SERVICE_NAME1)
                private String svcName;

                @Override public Object call() throws Exception {
                    fail();

                    return null;
                }
            });

            fail();
        }
        catch (IgniteCheckedException e) {
            assertTrue(e.getMessage().startsWith("Remote job threw user exception"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testClosureMethod() throws Exception {
        grid(0).compute().call(new IgniteCallable<Object>() {
            private DummyService svc;

            @IgniteServiceResource(serviceName = SERVICE_NAME1)
            private void service(DummyService svc) {
                assertNotNull(svc);

                assertTrue(svc instanceof ManagedService);

                this.svc = svc;
            }

            @Override public Object call() throws Exception {
                svc.noop();

                return null;
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testClosureMethodProxy() throws Exception {
        grid(0).compute(grid(0).cluster().forRemotes()).call(new IgniteCallable<Object>() {
            private DummyService svc;

            @IgniteServiceResource(serviceName = SERVICE_NAME2, proxyInterface = DummyService.class)
            private void service(DummyService svc) {
                assertNotNull(svc);

                // Ensure proxy instance.
                assertFalse(svc instanceof ManagedService);

                this.svc = svc;
            }

            @Override public Object call() throws Exception {
                svc.noop();

                return null;
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testClosureMethodLocalProxy() throws Exception {
        grid(0).compute(grid(0).cluster().forRemotes()).call(new IgniteCallable<Object>() {
            private DummyService svc;

            @IgniteServiceResource(serviceName = SERVICE_NAME1, proxyInterface = DummyService.class)
            private void service(DummyService svc) {
                assertNotNull(svc);

                // Ensure proxy instance.
                assertTrue(svc instanceof ManagedService);

                this.svc = svc;
            }

            @Override public Object call() throws Exception {
                svc.noop();

                return null;
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testClosureMethodWithIncorrectType() throws Exception {
        try {
            grid(0).compute().call(new IgniteCallable<Object>() {
                @IgniteServiceResource(serviceName = SERVICE_NAME1)
                private void service(String svcs) {
                    fail();
                }

                @Override public Object call() throws Exception {
                    return null;
                }
            });

            fail();
        }
        catch (IgniteCheckedException e) {
            assertTrue(e.getMessage().startsWith("Remote job threw user exception"));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testClosureFieldWithNonExistentService() throws Exception {
        grid(0).compute().call(new IgniteCallable<Object>() {
            @IgniteServiceResource(serviceName = "nonExistentService")
            private DummyService svc;

            @Override public Object call() throws Exception {
                assertNull(svc);

                return null;
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testClosureMethodWithNonExistentService() throws Exception {
        grid(0).compute().call(new IgniteCallable<Object>() {
            @IgniteServiceResource(serviceName = "nonExistentService")
            private void service(DummyService svc) {
                assertNull(svc);
            }

            @Override public Object call() throws Exception {
                return null;
            }
        });
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
    public static class DummyServiceImpl implements DummyService, ManagedService {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public void noop() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void cancel(ManagedServiceContext ctx) {
            System.out.println("Cancelling service: " + ctx.name());
        }

        /** {@inheritDoc} */
        @Override public void init(ManagedServiceContext ctx) throws Exception {
            System.out.println("Initializing service: " + ctx.name());
        }

        /** {@inheritDoc} */
        @Override public void execute(ManagedServiceContext ctx) {
            System.out.println("Executing service: " + ctx.name());
        }
    }
}
