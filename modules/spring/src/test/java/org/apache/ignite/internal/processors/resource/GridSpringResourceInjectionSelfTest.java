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

import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSpring;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.SpringResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Tests for injected resource.
 */
@SuppressWarnings("unused")
public class GridSpringResourceInjectionSelfTest extends GridCommonAbstractTest {
    /** Bean name. */
    private static final String DUMMY_BEAN = "dummyResourceBean";

    /** Test grid with Spring context. */
    private static Ignite grid;

    /** {@inheritDoc} */
    @Override public void beforeTestsStarted() throws Exception {
        grid = IgniteSpring.start(new ClassPathXmlApplicationContext(
            "/org/apache/ignite/internal/processors/resource/spring-resource.xml"));
    }

    /** {@inheritDoc} */
    @Override public void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testClosureFieldByResourceName() throws Exception {
        grid.compute().call(new IgniteCallable<Object>() {
            @SpringResource(resourceName = DUMMY_BEAN)
            private transient DummyResourceBean dummyRsrcBean;

            @Override public Object call() throws Exception {
                assertNotNull(dummyRsrcBean);

                return null;
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testClosureFieldByResourceClass() throws Exception {
        grid.compute().call(new IgniteCallable<Object>() {
            @SpringResource(resourceClass = DummyResourceBean.class)
            private transient DummyResourceBean dummyRsrcBean;

            @Override public Object call() throws Exception {
                assertNotNull(dummyRsrcBean);

                return null;
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testClosureFieldByResourceClassWithMultipleBeans() throws Exception {
        IgniteConfiguration anotherCfg = new IgniteConfiguration();
        anotherCfg.setGridName("anotherGrid");

        Ignite anotherGrid = IgniteSpring.start(anotherCfg, new ClassPathXmlApplicationContext(
            "/org/apache/ignite/internal/processors/resource/spring-resource-with-duplicate-beans.xml"));

        Throwable err = assertError(new IgniteCallable<Object>() {
            @SpringResource(resourceClass = DummyResourceBean.class)
            private transient DummyResourceBean dummyRsrcBean;

            @Override public Object call() throws Exception {
                assertNotNull(dummyRsrcBean);

                return null;
            }
        }, anotherGrid, null);

        assertTrue("Unexpected message: " + err.getMessage(), err.getMessage().startsWith("No qualifying bean of type " +
            "[org.apache.ignite.internal.processors.resource.GridSpringResourceInjectionSelfTest$DummyResourceBean]" +
            " is defined: expected single matching bean but found 2:"));

        G.stop("anotherGrid", false);
    }

    /**
     * Resource injection with non-existing resource name.
     */
    public void testClosureFieldWithWrongResourceName() {
        assertError(new IgniteCallable<Object>() {
            @SpringResource(resourceName = "nonExistentResource")
            private transient DummyResourceBean dummyRsrcBean;

            @Override public Object call() throws Exception {
                assertNull(dummyRsrcBean);

                return null;
            }
        }, "No bean named 'nonExistentResource' is defined");
    }

    /**
     * Resource injection with non-existing resource class.
     */
    public void testClosureFieldWithWrongResourceClass() {
        assertError(new IgniteCallable<Object>() {
            @SpringResource(resourceClass = AnotherDummyResourceBean.class)
            private transient AnotherDummyResourceBean dummyRsrcBean;

            @Override public Object call() throws Exception {
                assertNull(dummyRsrcBean);

                return null;
            }
        }, "No qualifying bean of type [org.apache.ignite.internal.processors.resource." +
            "GridSpringResourceInjectionSelfTest$AnotherDummyResourceBean] is defined");
    }

    /**
     * Resource injection with both resource and class set (ambiguity).
     */
    public void testClosureFieldByResourceClassAndName() {
        assertError(new IgniteCallable<Object>() {
            @SpringResource(resourceClass = DummyResourceBean.class, resourceName = DUMMY_BEAN)
            private transient DummyResourceBean dummyRsrcBean;

            @Override public Object call() throws Exception {
                assertNull(dummyRsrcBean);

                return null;
            }
        }, "Either bean name or its class must be specified in @SpringResource, but not both");
    }

    /**
     * Resource injection with no name and class set.
     */
    public void testClosureFieldWithNoParams() {
        assertError(new IgniteCallable<Object>() {
            @SpringResource
            private transient DummyResourceBean dummyRsrcBean;

            @Override public Object call() throws Exception {
                assertNull(dummyRsrcBean);

                return null;
            }
        }, "Either bean name or its class must be specified in @SpringResource, but not both");
    }

    /**
     * @throws Exception If failed.
     */
    public void testClosureMethodWithResourceName() throws Exception {
        grid.compute().call(new IgniteCallable<Object>() {
            private DummyResourceBean dummyRsrcBean;

            @SpringResource(resourceName = DUMMY_BEAN)
            private void setDummyResourceBean(DummyResourceBean dummyRsrcBean) {
                assertNotNull(dummyRsrcBean);

                this.dummyRsrcBean = dummyRsrcBean;
            }

            @Override public Object call() throws Exception {
                assertNotNull(dummyRsrcBean);

                return null;
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testClosureMethodWithResourceClass() throws Exception {
        grid.compute().call(new IgniteCallable<Object>() {
            private DummyResourceBean dummyRsrcBean;

            @SpringResource(resourceClass = DummyResourceBean.class)
            private void setDummyResourceBean(DummyResourceBean dummyRsrcBean) {
                assertNotNull(dummyRsrcBean);

                this.dummyRsrcBean = dummyRsrcBean;
            }

            @Override public Object call() throws Exception {
                assertNotNull(dummyRsrcBean);

                return null;
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testClosureMethodWithResourceClassWithMultipleBeans() throws Exception {
        IgniteConfiguration anotherCfg = new IgniteConfiguration();
        anotherCfg.setGridName("anotherGrid");

        Ignite anotherGrid = IgniteSpring.start(anotherCfg, new ClassPathXmlApplicationContext(
            "/org/apache/ignite/internal/processors/resource/spring-resource-with-duplicate-beans.xml"));

        try {
            Throwable err = assertError(new IgniteCallable<Object>() {
                private DummyResourceBean dummyRsrcBean;

                @SpringResource(resourceClass = DummyResourceBean.class)
                private void setDummyResourceBean(DummyResourceBean dummyRsrcBean) {
                    assertNotNull(dummyRsrcBean);

                    this.dummyRsrcBean = dummyRsrcBean;
                }

                @Override public Object call() throws Exception {
                    assertNotNull(dummyRsrcBean);

                    return null;
                }
            }, anotherGrid, null);

            assertTrue("Unexpected message: " + err.getMessage(), err.getMessage().startsWith("No qualifying bean of type " +
                "[org.apache.ignite.internal.processors.resource.GridSpringResourceInjectionSelfTest$DummyResourceBean]" +
                " is defined: expected single matching bean but found 2:"));
        }
        finally {
            G.stop("anotherGrid", false);
        }
    }

    /**
     * Resource injection with non-existing resource name.
     */
    public void testClosureMethodWithWrongResourceName() {
        assertError(new IgniteCallable<Object>() {
            private DummyResourceBean dummyRsrcBean;

            @SpringResource(resourceName = "nonExistentResource")
            private void setDummyResourceBean(DummyResourceBean dummyRsrcBean) {
                // No-op.
            }

            @Override public Object call() throws Exception {
                assertNull(dummyRsrcBean);

                return null;
            }
        }, "No bean named 'nonExistentResource' is defined");
    }

    /**
     * Resource injection with non-existing resource class.
     */
    public void testClosureMethodWithWrongResourceClass() {
        assertError(new IgniteCallable<Object>() {
            private AnotherDummyResourceBean dummyRsrcBean;

            @SpringResource(resourceClass = AnotherDummyResourceBean.class)
            private void setDummyResourceBean(AnotherDummyResourceBean dummyRsrcBean) {
                // No-op.
            }

            @Override public Object call() throws Exception {
                assertNull(dummyRsrcBean);

                return null;
            }
        }, "No qualifying bean of type [org.apache.ignite.internal.processors.resource" +
                ".GridSpringResourceInjectionSelfTest$AnotherDummyResourceBean] is defined");
    }

    /**
     * Resource injection with both resource and class set (ambiguity).
     */
    public void testClosureMethodByResourceClassAndName() {
        assertError(new IgniteCallable<Object>() {
            @SpringResource(resourceClass = DummyResourceBean.class, resourceName = DUMMY_BEAN)
            private transient DummyResourceBean dummyRsrcBean;

            @Override public Object call() throws Exception {
                assertNull(dummyRsrcBean);

                return null;
            }
        }, "Either bean name or its class must be specified in @SpringResource, but not both");
    }

    /**
     * Resource injection with no params.
     */
    public void testClosureMethodWithNoParams() {
        assertError(new IgniteCallable<Object>() {
            @SpringResource
            private transient DummyResourceBean dummyRsrcBean;

            @Override public Object call() throws Exception {
                assertNull(dummyRsrcBean);

                return null;
            }
        }, "Either bean name or its class must be specified in @SpringResource, but not both");
    }

    /**
     * @param job {@link IgniteCallable} to be run
     * @param grid Node.
     * @param expEMsg Message that {@link IgniteException} thrown from <tt>job</tt> should bear
     * @return Thrown error.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    private Throwable assertError(final IgniteCallable<?> job, final Ignite grid, String expEMsg) {
        return GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                grid.compute(grid.cluster().forLocal()).call(job);
                return null;
            }
        }, IgniteException.class, expEMsg);
    }

    /**
     * @param job {@link IgniteCallable} to be run
     * @param expEMsg Message that {@link IgniteException} thrown from <tt>job</tt> should bear
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    private void assertError(final IgniteCallable<?> job, String expEMsg) {
        assertError(job, grid, expEMsg);
    }

    /**
     * Dummy resource bean.
     */
    public static class DummyResourceBean {
        /**
         *
         */
        public DummyResourceBean() {
            // No-op.
        }
    }

    /**
     * Another dummy resource bean.
     */
    private static class AnotherDummyResourceBean {
        /**
         *
         */
        public AnotherDummyResourceBean() {
            // No-op.
        }
    }
}
