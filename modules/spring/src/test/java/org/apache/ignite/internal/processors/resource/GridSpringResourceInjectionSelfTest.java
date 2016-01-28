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
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Tests for injected resource.
 */
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
    public void testClosureField() throws Exception {
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
     * Resource injection with non-existing resource name.
     */
    public void testClosureFieldWithWrongResourceName() throws Exception {
        try {
            grid.compute().call(new IgniteCallable<Object>() {
                @SpringResource(resourceName = "")
                private transient DummyResourceBean dummyResourceBean;

                @Override public Object call() throws Exception {
                    assertNull(dummyResourceBean);

                    return null;
                }
            });
        }
        catch (IgniteException e) {
            if (e.getMessage().contains("No bean named '' is defined"))
                return;
        }

        fail();
    }

    /**
     * @throws Exception If failed.
     */
    public void testClosureMethod() throws Exception {
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
     * Resource injection with non-existing resource name.
     */
    public void testClosureMethodWithWrongResourceName() throws Exception {
        try {
            grid.compute().call(new IgniteCallable<Object>() {
                private DummyResourceBean dummyResourceBean;

                @SpringResource(resourceName = "")
                private void setDummyResourceBean(DummyResourceBean dummyResourceBean) {
                }

                @Override public Object call() throws Exception {
                    assertNull(dummyResourceBean);

                    return null;
                }
            });
        }
        catch (IgniteException e) {
            if (e.getMessage().contains("No bean named '' is defined"))
                return;
        }

        fail();
    }

    /**
     * Dummy resource bean.
     */
    public static class DummyResourceBean {
        public DummyResourceBean() {
        }
    }
}
