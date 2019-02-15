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

package org.apache.ignite.spi.discovery.tcp.ipfinder.gce;

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAbstractSelfTest;
import org.apache.ignite.testsuites.IgniteGCETestSuite;

/**
 * Google Cloud Storage based IP finder tests.
 */
public class TcpDiscoveryGoogleStorageIpFinderSelfTest
    extends TcpDiscoveryIpFinderAbstractSelfTest<TcpDiscoveryGoogleStorageIpFinder> {
    /** Bucket name. */
    private static String bucketName;

    /**
     * Constructor.
     *
     * @throws Exception If any error occurs.
     */
    public TcpDiscoveryGoogleStorageIpFinderSelfTest() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        bucketName = "ip-finder-test-bucket-" + InetAddress.getLocalHost().getAddress()[3];

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        try {
            Method method = TcpDiscoveryGoogleStorageIpFinder.class.getDeclaredMethod("removeBucket", String.class);

            method.setAccessible(true);

            method.invoke(finder, bucketName);
        }
        catch (Exception e) {
            log.warning("Failed to remove bucket on GCE [bucketName=" + bucketName + ", mes=" + e.getMessage() + ']');
        }
    }

    /** {@inheritDoc} */
    @Override protected TcpDiscoveryGoogleStorageIpFinder ipFinder() throws Exception {
        TcpDiscoveryGoogleStorageIpFinder finder = new TcpDiscoveryGoogleStorageIpFinder();

        injectLogger(finder);

        assert finder.isShared() : "Ip finder must be shared by default.";

        finder.setServiceAccountId(IgniteGCETestSuite.getServiceAccountId());
        finder.setServiceAccountP12FilePath(IgniteGCETestSuite.getP12FilePath());
        finder.setProjectName(IgniteGCETestSuite.getProjectName());

        // Bucket name must be unique across the whole GCE platform.
        finder.setBucketName(bucketName);

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