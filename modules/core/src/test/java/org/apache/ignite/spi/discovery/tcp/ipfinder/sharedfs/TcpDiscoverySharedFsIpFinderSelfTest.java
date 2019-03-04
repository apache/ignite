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

package org.apache.ignite.spi.discovery.tcp.ipfinder.sharedfs;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAbstractSelfTest;
import org.junit.Test;

/**
 * GridTcpDiscoverySharedFsIpFinder test.
 */
public class TcpDiscoverySharedFsIpFinderSelfTest
    extends TcpDiscoveryIpFinderAbstractSelfTest<TcpDiscoverySharedFsIpFinder> {
    /**
     * Constructor.
     *
     * @throws Exception If any error occurs.
     */
    public TcpDiscoverySharedFsIpFinderSelfTest() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected TcpDiscoverySharedFsIpFinder ipFinder() {
        TcpDiscoverySharedFsIpFinder finder = new TcpDiscoverySharedFsIpFinder();

        assert finder.isShared() : "Ip finder should be shared by default.";

        File tmpFile = new File(System.getProperty("java.io.tmpdir"), UUID.randomUUID().toString());

        assert !tmpFile.exists();

        if (!tmpFile.mkdir())
            assert false;

        finder.setPath(tmpFile.getAbsolutePath());

        return finder;
    }

    /**
     * @throws Exception If any error occurs.
     */
    @Test
    public void testUniqueNames() throws Exception {
        InetSocketAddress node1 = new InetSocketAddress("10.7.7.7", 4343);
        InetAddress ia = InetAddress.getByAddress("localhost", new byte[] {10, 7, 7, 7});
        InetSocketAddress node2 = new InetSocketAddress(ia, 4343);

        List<InetSocketAddress> initAddrs = Arrays.asList(node1, node2);

        finder.registerAddresses(initAddrs);

        assertEquals("Wrong collection size", 1, finder.getRegisteredAddresses().size());

        finder.unregisterAddresses(initAddrs);

        assertEquals("Wrong collection size", 0, finder.getRegisteredAddresses().size());

        finder.close();
    }
}
