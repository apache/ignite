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

package org.apache.ignite.util.mbeans;

import java.util.concurrent.Callable;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Disabling MBeans test.
 */
public class GridMBeanDisableSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        IgniteUtils.IGNITE_MBEANS_DISABLED = true;

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        IgniteUtils.IGNITE_MBEANS_DISABLED = false;
    }

    /**
     * Test MBean registration.
     *
     * @throws Exception Thrown if test fails.
     */
    public void testCorrectMBeanInfo() throws Exception {
        // Node should start and stopped with no errors.
        try (final Ignite ignite = startGrid(0)) {

            // Cache should be created and closed with no errors.
            try (IgniteCache cache = ignite.getOrCreateCache(new CacheConfiguration("MyCache"))) {

                final MBeanServer server = ignite.configuration().getMBeanServer();

                GridTestUtils.assertThrowsWithCause(
                    new Callable<Void>() {
                        @Override public Void call() throws Exception {
                            U.registerMBean(server, ignite.name(), "dummy", "DummyMbean1", new DummyMBeanImpl(), DummyMBean.class);

                            return null;

                        }
                    }, MBeanRegistrationException.class);

                GridTestUtils.assertThrowsWithCause(
                    new Callable<Void>() {
                        @Override public Void call() throws Exception {
                            ObjectName objName = U.makeMBeanName(
                                ignite.name(),
                                "dummy",
                                "DummyMbean2"
                            );

                            U.registerMBean(server, objName, new DummyMBeanImpl(), DummyMBean.class);

                            return null;

                        }
                    }, MBeanRegistrationException.class);

                GridTestUtils.assertThrowsWithCause(
                    new Callable<Void>() {
                        @Override public Void call() throws Exception {
                            U.registerCacheMBean(server, ignite.name(), "MyCache", "DummyMbean3",
                                new DummyMBeanImpl(), DummyMBean.class);

                            return null;

                        }
                    }, MBeanRegistrationException.class);
            }
        }
    }

    /**
     * MBean dummy interface.
     */
    interface DummyMBean {
        /** */
        void noop();
    }

    /**
     * MBean stub.
     */
    static class DummyMBeanImpl implements DummyMBean {
        /** {@inheritDoc} */
        @Override public void noop() {
            // No op.
        }
    }
}