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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridLoggerProxy;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for injected logger category.
 */
public class GridLoggerInjectionSelfTest extends GridCommonAbstractTest implements Externalizable {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * Test that closure gets right log category injected on all nodes using field injection.
     *
     * @throws Exception If failed.
     */
    public void testClosureField() throws Exception {
        Ignite ignite = grid(0);

        ignite.compute().call(new IgniteCallable<Object>() {
            @LoggerResource(categoryClass = GridLoggerInjectionSelfTest.class)
            private IgniteLogger log;

            @Override public Object call() throws Exception {
                if (log instanceof GridLoggerProxy) {
                    Object category = U.field(log,  "ctgr");

                    assertTrue("Logger created for the wrong category.",
                        category.toString().contains(GridLoggerInjectionSelfTest.class.getName()));
                }
                else
                    fail("This test should be run with proxy logger.");

                return null;
            }
        });
    }

    /**
     * Test that closure gets right log category injected on all nodes using method injection.
     *
     * @throws Exception If failed.
     */
    public void testClosureMethod() throws Exception {
        Ignite ignite = grid(0);

        ignite.compute().call(new IgniteCallable<Object>() {
            @LoggerResource(categoryClass = GridLoggerInjectionSelfTest.class)
            private void log(IgniteLogger log) {
                if (log instanceof GridLoggerProxy) {
                    Object category = U.field(log,  "ctgr");

                    assertTrue("Logger created for the wrong category.",
                        category.toString().contains(GridLoggerInjectionSelfTest.class.getName()));
                }
                else
                    fail("This test should be run with proxy logger.");
            }

            @Override public Object call() throws Exception {
                return null;
            }
        });
    }

    /**
     * Test that closure gets right log category injected through {@link org.apache.ignite.resources.LoggerResource#categoryName()}.
     *
     * @throws Exception If failed.
     */
    public void testStringCategory() throws Exception {
        Ignite ignite = grid(0);

        ignite.compute().call(new IgniteCallable<Object>() {
            @LoggerResource(categoryName = "GridLoggerInjectionSelfTest")
            private void log(IgniteLogger log) {
                if (log instanceof GridLoggerProxy) {
                    Object category = U.field(log,  "ctgr");

                    assertTrue("Logger created for the wrong category.",
                        "GridLoggerInjectionSelfTest".equals(category.toString()));
                }
                else
                    fail("This test should be run with proxy logger.");
            }

            @Override public Object call() throws Exception {
                return null;
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // No-op.
    }
}