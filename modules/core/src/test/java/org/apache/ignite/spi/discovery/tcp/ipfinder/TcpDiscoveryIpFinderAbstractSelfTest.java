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

package org.apache.ignite.spi.discovery.tcp.ipfinder;

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Abstract test for ip finder.
 */
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

    /**
     * @throws Exception If any error occurs.
     */
    public void testIpFinder() throws Exception {
        finder.initializeLocalAddresses(Arrays.asList(new InetSocketAddress(InetAddress.getLocalHost(), 1000)));

        InetSocketAddress node1 = new InetSocketAddress(InetAddress.getLocalHost(), 1000);
        InetSocketAddress node2 = new InetSocketAddress(InetAddress.getLocalHost(), 1001);

        List<InetSocketAddress> initAddrs = Arrays.asList(node1, node2);

        finder.registerAddresses(Collections.singletonList(node1));

        finder.registerAddresses(initAddrs);

        Collection<InetSocketAddress> addrs = finder.getRegisteredAddresses();

        for (int i = 0; i < 5 && addrs.size() != 2; i++) {
            U.sleep(1000);

            addrs = finder.getRegisteredAddresses();
        }

        assertEquals("Wrong collection size", 2, addrs.size());

        for (InetSocketAddress addr : initAddrs)
            assert addrs.contains(addr) : "Address is missing (got inconsistent addrs collection): " + addr;

        finder.unregisterAddresses(Collections.singletonList(node1));

        addrs = finder.getRegisteredAddresses();

        for (int i = 0; i < 5 && addrs.size() != 1; i++) {
            U.sleep(1000);

            addrs = finder.getRegisteredAddresses();
        }

        assertEquals("Wrong collection size", 1, addrs.size());

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