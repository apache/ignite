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

package org.apache.ignite.spi.discovery.tcp.ipfinder.s3;

import com.amazonaws.auth.BasicAWSCredentials;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAbstractSelfTest;
import org.apache.ignite.testsuites.IgniteS3TestSuite;

/**
 * TcpDiscoveryS3IpFinder test.
 */
public class TcpDiscoveryS3IpFinderSelfTest
    extends TcpDiscoveryIpFinderAbstractSelfTest<TcpDiscoveryS3IpFinder> {
    /**
     * Constructor.
     *
     * @throws Exception If any error occurs.
     */
    public TcpDiscoveryS3IpFinderSelfTest() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected TcpDiscoveryS3IpFinder ipFinder() throws Exception {
        TcpDiscoveryS3IpFinder finder = new TcpDiscoveryS3IpFinder();

        injectLogger(finder);

        assert finder.isShared() : "Ip finder should be shared by default.";

        finder.setAwsCredentials(new BasicAWSCredentials(IgniteS3TestSuite.getAccessKey(),
            IgniteS3TestSuite.getSecretKey()));

        // Bucket name should be unique for the host to parallel test run on one bucket.
        finder.setBucketName("ip-finder-unit-test-bucket-" + InetAddress.getLocalHost().getAddress()[3]);

        for (int i = 0; i < 5; i++) {
            Collection<InetSocketAddress> addrs = finder.getRegisteredAddresses();

            if (!addrs.isEmpty())
                finder.unregisterAddresses(addrs);
            else
                return finder;

            U.sleep(1000);
        }

        if (!finder.getRegisteredAddresses().isEmpty())
            throw new Exception("Failed to initialize IP finder.");

        return finder;
    }
}