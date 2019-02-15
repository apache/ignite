/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.util.mbeans;

import java.util.concurrent.Callable;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Disabling MBeans test.
 */
public class GridMBeanDisableSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override public void setUp() throws Exception {
        IgniteUtils.IGNITE_MBEANS_DISABLED = true;

        super.setUp();
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        IgniteUtils.IGNITE_MBEANS_DISABLED = false;

        super.tearDown();
    }

    /**
     * Test MBean registration.
     *
     * @throws Exception Thrown if test fails.
     */
    @Test
    public void testCorrectMBeanInfo() throws Exception {
        // Node should start and stopped with no errors.
        try (final Ignite ignite = startGrid(0)) {
            final MBeanServer srv = ignite.configuration().getMBeanServer();

            GridTestUtils.assertThrowsWithCause(
                new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        U.registerMBean(srv, ignite.name(), "dummy", "DummyMbean1", new DummyMBeanImpl(), DummyMBean.class);

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

                        U.registerMBean(srv, objName, new DummyMBeanImpl(), DummyMBean.class);

                        return null;

                    }
                }, MBeanRegistrationException.class);
        }
    }

    /** Check that a cache can be started when MBeans are disabled. */
    @Test
    public void testCacheStart() throws Exception {
        try (
            Ignite ignite = startGrid(0);
            IgniteCache<String, String> cache = ignite.getOrCreateCache("MyCache")
        ) {
            cache.put("foo", "bar");
            assertEquals("bar", cache.get("foo"));
        }
    }

    /**
     * MBean dummy interface.
     */
    public interface DummyMBean {
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
