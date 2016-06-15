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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSpring;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.SpringResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.concurrent.Callable;

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
            private transient DummyResourceBean dummyResourceBean;

            @Override public Object call() throws Exception {
                assertNotNull(dummyResourceBean);

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
            private transient DummyResourceBean dummyResourceBean;

            @Override public Object call() throws Exception {
                assertNotNull(dummyResourceBean);

                return null;
            }
        });
    }

    /**
     * Resource injection with non-existing resource name.
     */
    public void testClosureFieldWithWrongResourceName() throws Exception {
        assertError(new IgniteCallable<Object>() {
            @SpringResource(resourceName = "nonExistentResource")
            private transient DummyResourceBean dummyResourceBean;

            @Override public Object call() throws Exception {
                assertNull(dummyResourceBean);

                return null;
            }
        }, "No bean named 'nonExistentResource' is defined");
    }

    /**
     * Resource injection with non-existing resource class.
     */
    public void testClosureFieldWithWrongResourceClass() throws Exception {
        assertError(new IgniteCallable<Object>() {
            @SpringResource(resourceClass = AnotherDummyResourceBean.class)
            private transient AnotherDummyResourceBean dummyResourceBean;

            @Override public Object call() throws Exception {
                assertNull(dummyResourceBean);

                return null;
            }
        }, "No qualifying bean of type [org.apache.ignite.internal.processors.resource." +
            "GridSpringResourceInjectionSelfTest$AnotherDummyResourceBean] is defined");
    }

    /**
     * Resource injection with both resource and class set (ambiguity).
     */
    public void testClosureFieldByResourceClassAndName() throws Exception {
        assertError(new IgniteCallable<Object>() {
            @SpringResource(resourceClass = DummyResourceBean.class, resourceName = DUMMY_BEAN)
            private transient DummyResourceBean dummyResourceBean;

            @Override public Object call() throws Exception {
                assertNull(dummyResourceBean);

                return null;
            }
        }, "Either bean name or its class must be specified in @SpringResource, but not both");
    }

    /**
     * Resource injection with no name and class set.
     */
    public void testClosureFieldWithNoParams() throws Exception {
        assertError(new IgniteCallable<Object>() {
            @SpringResource
            private transient DummyResourceBean dummyResourceBean;

            @Override public Object call() throws Exception {
                assertNull(dummyResourceBean);

                return null;
            }
        }, "Either bean name or its class must be specified in @SpringResource, but not both");
    }

    /**
     * @throws Exception If failed.
     */
    public void testClosureMethodWithResourceName() throws Exception {
        grid.compute().call(new IgniteCallable<Object>() {
            private DummyResourceBean dummyResourceBean;

            @SpringResource(resourceName = DUMMY_BEAN)
            private void setDummyResourceBean(DummyResourceBean dummyResourceBean) {
                assertNotNull(dummyResourceBean);

                this.dummyResourceBean = dummyResourceBean;
            }

            @Override public Object call() throws Exception {
                return null;
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testClosureMethodWithResourceClass() throws Exception {
        grid.compute().call(new IgniteCallable<Object>() {
            private DummyResourceBean dummyResourceBean;

            @SpringResource(resourceClass = DummyResourceBean.class)
            private void setDummyResourceBean(DummyResourceBean dummyResourceBean) {
                assertNotNull(dummyResourceBean);

                this.dummyResourceBean = dummyResourceBean;
            }

            @Override public Object call() throws Exception {
                return null;
            }
        });
    }

    /**
     * Resource injection with non-existing resource name.
     */
    public void testClosureMethodWithWrongResourceName() throws Exception {
        assertError(new IgniteCallable<Object>() {
            private DummyResourceBean dummyResourceBean;

            @SpringResource(resourceName = "nonExistentResource")
            private void setDummyResourceBean(DummyResourceBean dummyResourceBean) {
            }

            @Override public Object call() throws Exception {
                assertNull(dummyResourceBean);

                return null;
            }
        }, "No bean named 'nonExistentResource' is defined");
    }

    /**
     * Resource injection with non-existing resource class.
     */
    public void testClosureMethodWithWrongResourceClass() throws Exception {
        assertError(new IgniteCallable<Object>() {
            private AnotherDummyResourceBean dummyResourceBean;

            @SpringResource(resourceClass = AnotherDummyResourceBean.class)
            private void setDummyResourceBean(AnotherDummyResourceBean dummyResourceBean) {
            }

            @Override public Object call() throws Exception {
                assertNull(dummyResourceBean);

                return null;
            }
        }, "No qualifying bean of type [org.apache.ignite.internal.processors.resource" +
                ".GridSpringResourceInjectionSelfTest$AnotherDummyResourceBean] is defined");
    }

    /**
     * Resource injection with both resource and class set (ambiguity).
     */
    public void testClosureMethodByResourceClassAndName() throws Exception {
        assertError(new IgniteCallable<Object>() {
            @SpringResource(resourceClass = DummyResourceBean.class, resourceName = DUMMY_BEAN)
            private transient DummyResourceBean dummyResourceBean;

            @Override public Object call() throws Exception {
                assertNull(dummyResourceBean);

                return null;
            }
        }, "Either bean name or its class must be specified in @SpringResource, but not both");
    }

    /**
     * Resource injection with no params.
     */
    public void testClosureMethodWithNoParams() throws Exception {
        assertError(new IgniteCallable<Object>() {
            @SpringResource
            private transient DummyResourceBean dummyResourceBean;

            @Override public Object call() throws Exception {
                assertNull(dummyResourceBean);

                return null;
            }
        }, "Either bean name or its class must be specified in @SpringResource, but not both");
    }

    /**
     * @param job {@link IgniteCallable} to be run
     * @param expectedExceptionMessage Message that {@link Throwable} thrown from <tt>job</tt> should bear
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    private void assertError(IgniteCallable<?> job,
                             String expectedExceptionMessage) {
        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                grid.compute().call(job);
                return null;
            }
        }, IgniteException.class, expectedExceptionMessage);
    }

    /**
     * Dummy resource bean.
     */
    public static class DummyResourceBean {
        public DummyResourceBean() {
        }
    }

    /**
     * Another dummy resource bean.
     */
    private static class AnotherDummyResourceBean {
        public AnotherDummyResourceBean() {
        }
    }
}
