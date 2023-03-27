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

package org.apache.ignite.spi.discovery.tcp.ipfinder.vm;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**  */
public class TcpDiscoveryVmIpFinderDnsResolveTest extends GridCommonAbstractTest {

    /** Fqnd */
    private static String FQDN = "test.domain";

    /** Multiple Fqnd */
    private static String MULTI_FQDN = "multi.test.domain";

    /** Incorrect fqnd */
    private static String BAD_FQDN = "bad.domain";

    /** Incorrect ip */
    private static String BAD_IP = "555.0.0.0";

    /** Incorrect ipv6 */
    private static String BAD_IP_6 = "[oo00::0000:ooo:ooo:ooo]";

    /** local host address */
    private static String LOCAL_HOST = "localhost";

    /** Ip1 */
    private static String IP1 = "10.0.0.1";

    /** Ip2 */
    private static String IP2 = "10.0.0.2";

    /** DNS service */
    private static TwoIpRoundRobinDnsService hostNameSvc;

    /** original DNS */
    private static Object nameSvc;

    /**
     */
    @BeforeClass
    public static void before() throws Exception {
        hostNameSvc = new TwoIpRoundRobinDnsService(FQDN, MULTI_FQDN, IP1, IP2);

        INameService.install(hostNameSvc);
    }

    /**
     */
    @AfterClass
    public static void cleanup() throws Exception {
        INameService.uninstall();
    }

    /**
     * Current test checks that in case if DNS will not be able to resolve hostname,
     * then no exceptions were thrown from getRegisteredAddresses method.
     */
    @Test
    public void testFqdnResolveWhenDnsCantResolveHostName() {
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        Set<String> addrs = new HashSet<>();

        addrs.add(BAD_FQDN);
        addrs.add(BAD_FQDN + ":47500");
        addrs.add(BAD_FQDN + ":47501..47502");

        addrs.add(BAD_IP);
        addrs.add(BAD_IP + ":47500");
        addrs.add(BAD_IP + ":47501..47502");

        addrs.add(BAD_IP_6);
        addrs.add(BAD_IP_6 + ":47500");
        addrs.add(BAD_IP_6 + ":47501..47502");

        ipFinder.setAddresses(addrs);

        Collection<InetSocketAddress> resolved = ipFinder.getRegisteredAddresses();

        assertNotNull(resolved);

        //for every host 4 entries (for 0, 47500, 47501, 47502)
        assertEquals(resolved.size(), 12);

        Iterator<InetSocketAddress> it = resolved.iterator();

        System.out.println("Resolved addresses " + resolved);

        while (it.hasNext()) {
            InetSocketAddress addr = it.next();

            assertNull(addr.getAddress());
        }
    }

    /**
     * Current test checks that TcpDiscoveryVmIpFinder will return new IP if DNS service will change the FQDN resolution.
     *
     * @param fqdn - A fully qualified domain name.
     * @param expectedCount - how many addresses should be resolved.
     * @throws Exception on error.
     */
    public void fqdnResolveAfterDnsHostChange(String fqdn, int expectedCount) throws Exception {
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        Set<String> addrs = new HashSet<>();

        addrs.add(fqdn);

        ipFinder.setAddresses(addrs);

        Collection<InetSocketAddress> resolved1 = ipFinder.getRegisteredAddresses();
        
        assertEquals(expectedCount, resolved1.size());

        InetSocketAddress addr1 = resolved1.iterator().next();

        //because of JAVA networkaddress cache ttl can be turn on.
        //will be great to change current test and run it in separate JVM.
        //and set there -Dsun.net.inetaddr.ttl=0 -Dsun.net.inetaddr.negative.ttl=0
        Thread.sleep(50_000);

        Collection<InetSocketAddress> resolved2 = ipFinder.getRegisteredAddresses();

        assertEquals(expectedCount, resolved2.size());

        InetSocketAddress addr2 = resolved2.iterator().next();

        log.info("Adrrs1 - " + addr1.getAddress() + " Adrrs2 - " + addr2.getAddress());

        assertFalse("Addresses weren't resolved second time. Probably DNS cache has TTL more then 1 min, if yes " +
                "then please mute this test. Adrrs1 - " + addr1.getAddress() + " Adrrs2 - " + addr2.getAddress(),
            addr1.equals(addr2));
    }

    /**
     * Current test checks that TcpDiscoveryVmIpFinder will return new IP if DNS service will change the FQDN resolution.
     * There are no ports set, only hostname.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testFqdnResolveAfterDnsHostChange() throws Exception {
        fqdnResolveAfterDnsHostChange(FQDN, 1);
    }

    /**
     * Current test checks that TcpDiscoveryVmIpFinder will return new IP if DNS service will change the FQDN resolution.
     * Port will be used.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testFqdnWithPortResolveAfterDnsHostChange() throws Exception {
        fqdnResolveAfterDnsHostChange(FQDN + ":47500", 1);
    }

    /**
     * Current test checks that TcpDiscoveryVmIpFinder will return new IP if DNS service will change the FQDN resolution.
     * Port range will be used,
     *
     * @throws Exception if failed.
     */
    @Test
    public void testFqdnWithPortRangeResolveAfterDnsHostChange() throws Exception {
        fqdnResolveAfterDnsHostChange(FQDN + ":47500..47501", 2);
    }

    /**
     * Current test checks that in case of roundrobin DNS server that returns two IP if we set the FQDN name with port range
     * then every pair of host/port addresses will have the same IP (not different).
     */
    @Test
    public void testFqdnWithPortRangeResolveWithTwoIpRoundRobinDns() {
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        Set<String> addrs = new HashSet<>();

        addrs.add(FQDN + ":47500..47509");

        ipFinder.setAddresses(addrs);

        Collection<InetSocketAddress> resolved = ipFinder.getRegisteredAddresses();

        log.info("Resolved addresses are " + resolved);

        assertTrue(resolved.size() == 10);

        Iterator<InetSocketAddress> it = resolved.iterator();

        InetSocketAddress first = it.next();

        String ip = first.getAddress().getHostAddress();

        String host = first.getHostName();

        while (it.hasNext()) {
            InetSocketAddress curr = it.next();

            assertTrue("IP address isn't the same. ip - " + curr.getAddress().getHostAddress() + " expected " + ip,
                ip.equals(curr.getAddress().getHostAddress()));

            assertTrue("FQDN isn't the same. cur - " + curr.getHostName() + " expected " + host,
                host.equals(curr.getHostName()));
        }
    }

    /**
     * Current test checks that in case if TcpDiscoveryVmIpFinder has FQDN name and additional one "registeredAddress"
     * then both of them can be resolved.
     *
     * @param fgdn - A fully qualified domain name.
     * @param expectedSize - how many addresses should be resolved.
     * @param expectedDomainName - expected FQDN name.
     */
    public void fqdnResolveWithRegisteredAddrs(String fgdn, int expectedSize, String expectedDomainName) {
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        Set<String> addrs = new HashSet<>();

        addrs.add(fgdn);

        ipFinder.setAddresses(addrs);

        Collection<InetSocketAddress> registerAddrs = new HashSet<>();

        registerAddrs.add(new InetSocketAddress(LOCAL_HOST, 0));

        ipFinder.registerAddresses(registerAddrs);

        Collection<InetSocketAddress> resolved = ipFinder.getRegisteredAddresses();

        assertTrue(resolved.size() == expectedSize);

        log.info("Resolved addresses are " + resolved);

        Iterator<InetSocketAddress> it = resolved.iterator();

        while (it.hasNext()) {
            InetSocketAddress addr = it.next();

            assertNotNull(addr);

            assertTrue(expectedDomainName.equals(addr.getHostName()) || LOCAL_HOST.equals(addr.getHostName()));
        }
    }

    /**
     * Current test checks that in case if TcpDiscoveryVmIpFinder has FQDN name and additional one "registeredAddress"
     * then both of them can be resolved. There are no ports will be used, only hostname.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testFqdnResolveWithRegisteredAddrs() throws Exception {
        fqdnResolveWithRegisteredAddrs(FQDN, 2, FQDN);
    }

    /**
     * Current test checks that in case if TcpDiscoveryVmIpFinder has MULTI_FQDN (resolved to two ip) name
     * and additional one "registeredAddress" then both of them can be resolved. There are no ports will be used, only hostname.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testMultiFqdnResolveWithRegisteredAddrs() throws Exception {
        fqdnResolveWithRegisteredAddrs(MULTI_FQDN, 3, MULTI_FQDN);
    }

    /**
     * Current test checks that in case if TcpDiscoveryVmIpFinder has FQDN name and additional one "registeredAddress"
     * then both of them can be resolved. Port will be used.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testFqdnWithPortResolveWithRegisteredAddrs() throws Exception {
        fqdnResolveWithRegisteredAddrs(FQDN + ":47500", 2, FQDN);
    }

    /**
     * Current test checks that in case if TcpDiscoveryVmIpFinder has MULTI_FQDN (resolved to two ip) name
     * and additional one "registeredAddress" then both of them can be resolved. Port will be used.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testMultiFqdnWithPortResolveWithRegisteredAddrs() throws Exception {
        fqdnResolveWithRegisteredAddrs(MULTI_FQDN + ":47500", 3, MULTI_FQDN);
    }

    /**
     * Current test checks that in case if TcpDiscoveryVmIpFinder has FQDN name and additional one "registeredAddress"
     * then both of them can be resolved. Port range will be used.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testFqdnWithPortRangeResolveWithRegisteredAddrs() throws Exception {
        fqdnResolveWithRegisteredAddrs(FQDN + ":47500..47501", 3, FQDN);
    }

    /**
     * Current test checks that in case if TcpDiscoveryVmIpFinder has MULTI_FQDN (resolved to two ip) name
     * and additional one "registeredAddress" then both of them can be resolved. Port range will be used.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testMultiFqdnWithPortRangeResolveWithRegisteredAddrs() throws Exception {
        fqdnResolveWithRegisteredAddrs(MULTI_FQDN + ":47500..47501", 5, MULTI_FQDN);
    }

    /**
     * Current test checks that in case if FQDN name with port range can be resolved in several IP addresses at the same time
     * then should be returned set with full ip/port sets for all ip addresses that can be resolved.
     */
    @Test
    public void testMultiFqdnResolveWithPortRange() {
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        Collection<String> addrs = new ArrayList<>();

        addrs.add(MULTI_FQDN + ":47500..47509");

        ipFinder.setAddresses(addrs);

        Collection<InetSocketAddress> resolved = ipFinder.getRegisteredAddresses();

        assertTrue(resolved.size() == 20);

        log.info("Resolved addresses are " + resolved);

        Iterator<InetSocketAddress> it = resolved.iterator();

        int cntIp1 = 0;

        int cntIp2 = 0;

        while (it.hasNext()) {
            InetSocketAddress curr = it.next();

            assertTrue(MULTI_FQDN.equals(curr.getHostName()));

            if (IP1.equals(curr.getAddress().getHostAddress()))
                cntIp1++;
            if (IP2.equals(curr.getAddress().getHostAddress()))
                cntIp2++;
        }

        assertTrue(cntIp1 == 10);

        assertTrue(cntIp2 == 10);
    }

    /**
     * Current test checks that if FQDN name can be resolved in several IP addresses at the same time
     * then all of them should be returned.
     *
     * @param fqdn - A fully qualified domain name.
     */
    public void multiFqdnResolve(String fqdn) {
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();

        Collection<String> addrs = new ArrayList<>();

        addrs.add(fqdn);

        ipFinder.setAddresses(addrs);

        Collection<InetSocketAddress> resolved = ipFinder.getRegisteredAddresses();

        assertTrue(resolved.size() == 2);

        log.info("Resolved addresses are " + resolved);

        Iterator<InetSocketAddress> it = resolved.iterator();

        InetSocketAddress addr1 = it.next();

        InetSocketAddress addr2 = it.next();

        assertNotNull(addr1);

        assertNotNull(addr2);

        assertTrue(MULTI_FQDN.equals(addr1.getHostName()) && MULTI_FQDN.equals(addr2.getHostName()));

        assertFalse("Addresses are the same. Adrrs1 - " + addr1.getAddress() +
            " Adrrs2 - " + addr2.getAddress(), addr1.equals(addr2));
    }

    /**
     * Current test checks that if FQDN name can be resolved in several IP addresses at the same time
     * then all of them should be returned. There are no ports, only host name
     */
    @Test
    public void testMultiFqdnResolve() {
        multiFqdnResolve(MULTI_FQDN);
    }

    /**
     * Current test checks that if FQDN name can be resolved in several IP addresses at the same time
     * then all of them should be returned. Only one port will be set.
     */
    @Test
    public void testMultiFqdnWithPortResolve() {
        multiFqdnResolve(MULTI_FQDN + ":47500");
    }

    /**
     * Custom hostname service.
     */
    @SuppressWarnings("restriction")
    public static class TwoIpRoundRobinDnsService implements INameService {
        /** ip1 */
        private final String ip1;

        /** ip2 */
        private final String ip2;

        /** fqdn */
        private final String fqdn;

        /** multiple fqdn */
        private final String multipleFqdn;

        /** change flag */
        private boolean needReturnIp1 = false;

        /** */
        public TwoIpRoundRobinDnsService(String fqdn, String multipleFqdn, String ip1, String ip2) {
            this.multipleFqdn = multipleFqdn;
            this.ip1 = ip1;
            this.ip2 = ip2;
            this.fqdn = fqdn;
        }

        /** {@inheritDoc} */
        @Override public InetAddress[] lookupAllHostAddr(String paramStr) throws UnknownHostException {
            if (fqdn.equals(paramStr)) {
                String ip = needReturnIp1 ? ip1 : ip2;

                needReturnIp1 = !needReturnIp1;

                final byte[] arrOfByte = sun.net.util.IPAddressUtil.textToNumericFormatV4(ip);

                final InetAddress addr = InetAddress.getByAddress(paramStr, arrOfByte);

                return new InetAddress[] {addr};
            }
            else if (multipleFqdn.equals(paramStr)) {
                final byte[] arrOfByte1 = sun.net.util.IPAddressUtil.textToNumericFormatV4(ip1);

                final byte[] arrOfByte2 = sun.net.util.IPAddressUtil.textToNumericFormatV4(ip2);

                final InetAddress addr1 = InetAddress.getByAddress(paramStr, arrOfByte1);

                final InetAddress addr2 = InetAddress.getByAddress(paramStr, arrOfByte2);

                return new InetAddress[] {addr1, addr2};
            }
            else
                throw new UnknownHostException();
        }

        /** {@inheritDoc} */
        @Override public String getHostByAddr(byte[] paramArrOfByte) throws UnknownHostException {
            throw new UnknownHostException();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            TwoIpRoundRobinDnsService svc = (TwoIpRoundRobinDnsService)o;
            return needReturnIp1 == svc.needReturnIp1 &&
                Objects.equals(ip1, svc.ip1) &&
                Objects.equals(ip2, svc.ip2) &&
                Objects.equals(fqdn, svc.fqdn) &&
                Objects.equals(multipleFqdn, svc.multipleFqdn);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {

            return Objects.hash(ip1, ip2, fqdn, multipleFqdn, needReturnIp1);
        }
    }

    /** */
    public interface INameService extends InvocationHandler {
        /** */
        static void install(
            INameService dns
        ) throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException, SecurityException, ClassNotFoundException {
            final Class<?> inetAddrCls = InetAddress.class;

            Object neu;

            Field nameSvcField;

            try {
                //JAVA 9+ class
                final Class<?> iface = Class.forName("java.net.InetAddress$NameService");

                nameSvcField = inetAddrCls.getDeclaredField("nameService");

                neu = Proxy.newProxyInstance(iface.getClassLoader(), new Class<?>[] {iface}, dns);
            }
            catch (final ClassNotFoundException | NoSuchFieldException e) {
                //JAVA <8 class
                nameSvcField = inetAddrCls.getDeclaredField("nameServices");

                final Class<?> iface = Class.forName("sun.net.spi.nameservice.NameService");

                neu = Collections.singletonList(Proxy.newProxyInstance(iface.getClassLoader(), new Class<?>[] {iface}, dns));
            }

            nameSvcField.setAccessible(true);

            nameSvc = nameSvcField.get(inetAddrCls);

            nameSvcField.set(inetAddrCls, neu);
        }

        /** */
        static void uninstall() throws IllegalArgumentException, IllegalAccessException, NoSuchFieldException, SecurityException {
            final Class<?> inetAddrCls = InetAddress.class;

            Field nameSvcField;

            try {
                //JAVA 9+ class
                Class.forName("java.net.InetAddress$NameService");

                nameSvcField = inetAddrCls.getDeclaredField("nameService");
            }
            catch (final ClassNotFoundException | NoSuchFieldException e) {
                //JAVA <8 class
                nameSvcField = inetAddrCls.getDeclaredField("nameServices");
            }

            nameSvcField.setAccessible(true);

            nameSvcField.set(inetAddrCls, nameSvc);
        }

        /**
         * Lookup a host mapping by name. Retrieve the IP addresses associated with a host
         *
         * @param host the specified hostname
         * @return array of IP addresses for the requested host
         * @throws UnknownHostException if no IP address for the {@code host} could be found
         */
        InetAddress[] lookupAllHostAddr(final String host) throws UnknownHostException;

        /**
         * Lookup the host corresponding to the IP address provided
         *
         * @param addr byte array representing an IP address
         * @return {@code String} representing the host name mapping
         * @throws UnknownHostException if no host found for the specified IP address
         */
        String getHostByAddr(final byte[] addr) throws UnknownHostException;

        /** */
        @Override default Object invoke(final Object proxy, final Method method,
            final Object[] args) throws Throwable {
            switch (method.getName()) {
                case "lookupAllHostAddr":
                    return lookupAllHostAddr((String)args[0]);
                case "getHostByAddr":
                    return getHostByAddr((byte[])args[0]);
                default:
                    final StringBuilder o = new StringBuilder();

                    o.append(method.getReturnType().getCanonicalName() + " " + method.getName() + "(");

                    final Class<?>[] ps = method.getParameterTypes();

                    for (int i = 0; i < ps.length; ++i) {
                        if (i > 0)
                            o.append(", ");

                        o.append(ps[i].getCanonicalName()).append(" p").append(i);
                    }

                    o.append(")");

                    throw new UnsupportedOperationException(o.toString());
            }
        }
    }
}
