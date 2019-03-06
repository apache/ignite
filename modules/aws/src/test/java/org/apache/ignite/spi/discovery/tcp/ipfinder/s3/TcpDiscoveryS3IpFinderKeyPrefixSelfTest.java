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

package org.apache.ignite.spi.discovery.tcp.ipfinder.s3;

import com.amazonaws.auth.BasicAWSCredentials;
import org.apache.ignite.spi.discovery.tcp.ipfinder.s3.client.DummyS3Client;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * TcpDiscoveryS3IpFinder tests key prefix for IP finder. For information about key prefix visit:
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/dev/ListingKeysHierarchy.html"/>.
 */
public class TcpDiscoveryS3IpFinderKeyPrefixSelfTest extends TcpDiscoveryS3IpFinderAbstractSelfTest {
    /**
     * Constructor.
     *
     * @throws Exception If any error occurs.
     */
    public TcpDiscoveryS3IpFinderKeyPrefixSelfTest() throws Exception {
    }

    /** {@inheritDoc} */
    @Override protected void setAwsCredentials(TcpDiscoveryS3IpFinder finder) {
        finder.setAwsCredentials(new BasicAWSCredentials("dummy", "dummy"));
    }

    /** {@inheritDoc} */
    @Override protected void setKeyPrefix(TcpDiscoveryS3IpFinder finder) {
        finder.setKeyPrefix("/test/key/prefix");
    }

    /** {@inheritDoc} */
    @Override protected TcpDiscoveryS3IpFinder ipFinder() throws Exception {
        TcpDiscoveryS3IpFinder ipFinder = Mockito.spy(new TcpDiscoveryS3IpFinder());

        Mockito.doReturn(new DummyS3Client()).when(ipFinder).createAmazonS3Client();

        setAwsCredentials(ipFinder);
        setBucketName(ipFinder);
        setKeyPrefix(ipFinder);

        return ipFinder;
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testIpFinder() throws Exception {
        injectLogger(finder);

        assert finder.isShared() : "Ip finder should be shared by default.";

        super.testIpFinder();
    }
}
