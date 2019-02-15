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

package org.apache.ignite.spi.discovery.tcp.ipfinder;

import java.lang.reflect.Field;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Abstract test for ip finder.
 */
@RunWith(JUnit4.class)
public abstract class TcpDiscoveryIpFinderAbstractSelfTest<T extends TcpDiscoveryIpFinder>
    extends GridCommonAbstractTest {
    /** */
    protected T finder;

    /**
     * Constructor.
     *
     * @throws Exception If any error occurs.
     */
    @SuppressWarnings({"AbstractMethodCallInConstructor", "OverriddenMethodCallDuringObjectConstruction"})
    protected TcpDiscoveryIpFinderAbstractSelfTest() throws Exception {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        finder = ipFinder();

        injectLogger(finder);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If any error occurs.
     */
    @Test
    public void testIpFinder() throws Exception {
        finder.initializeLocalAddresses(Arrays.asList(new InetSocketAddress(InetAddress.getLocalHost(), 1000)));

        InetSocketAddress node1 = new InetSocketAddress(InetAddress.getLocalHost(), 1000);
        InetSocketAddress node2 = new InetSocketAddress(InetAddress.getLocalHost(), 1001);
        InetSocketAddress node3 = new InetSocketAddress(
            Inet6Address.getByName("2001:0db8:85a3:08d3:1319:47ff:fe3b:7fd3"), 1002);

        List<InetSocketAddress> initAddrs = Arrays.asList(node1, node2, node3);

        finder.registerAddresses(Collections.singletonList(node1));

        finder.registerAddresses(initAddrs);

        Collection<InetSocketAddress> addrs = finder.getRegisteredAddresses();

        for (int i = 0; i < 5 && addrs.size() != 3; i++) {
            U.sleep(1000);

            addrs = finder.getRegisteredAddresses();
        }

        assertEquals("Wrong collection size", 3, addrs.size());

        for (InetSocketAddress addr : initAddrs)
            assert addrs.contains(addr) : "Address is missing (got inconsistent addrs collection): " + addr;

        finder.unregisterAddresses(Collections.singletonList(node2));

        addrs = finder.getRegisteredAddresses();

        for (int i = 0; i < 5 && addrs.size() != 2; i++) {
            U.sleep(1000);

            addrs = finder.getRegisteredAddresses();
        }

        assertEquals("Wrong collection size", 2, addrs.size());

        finder.unregisterAddresses(finder.getRegisteredAddresses());

        finder.close();
    }

    /**
     * @param finder IP finder.
     * @throws IllegalAccessException If any error occurs.
     */
    protected void injectLogger(T finder) throws IllegalAccessException {
        assert finder != null;

        for (Class cls = finder.getClass(); cls != Object.class; cls = cls.getSuperclass())
            for (Field fld : cls.getDeclaredFields())
                if (fld.getAnnotation(LoggerResource.class) != null) {
                    boolean accessible = fld.isAccessible();

                    fld.setAccessible(true);

                    fld.set(finder, log);

                    fld.setAccessible(accessible);
                }
    }

    /**
     * Creates and initializes ip finder.
     *
     * @return IP finder.
     * @throws Exception If any error occurs.
     */
    protected abstract T ipFinder() throws Exception;
}
