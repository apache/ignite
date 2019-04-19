/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.discovery.tcp.ipfinder.s3;

import com.amazonaws.auth.BasicAWSCredentials;
import org.apache.ignite.spi.discovery.tcp.ipfinder.s3.client.DummyS3Client;
import org.apache.ignite.spi.discovery.tcp.ipfinder.s3.encrypt.EncryptionService;
import org.apache.ignite.spi.discovery.tcp.ipfinder.s3.encrypt.MockEncryptionService;
import org.mockito.Mockito;

/**
 * TcpDiscoveryS3IpFinder tests client side encryption for S3 IP finder.
 */
public class TcpDiscoveryS3IpFinderClientSideEncryptionSelfTest extends TcpDiscoveryS3IpFinderAbstractSelfTest {
    /**
     * Constructor.
     *
     * @throws Exception If any error occurs.
     */
    public TcpDiscoveryS3IpFinderClientSideEncryptionSelfTest() throws Exception {
    }

    /** {@inheritDoc} */
    @Override protected void setAwsCredentials(TcpDiscoveryS3IpFinder finder) {
        finder.setAwsCredentials(new BasicAWSCredentials("dummy", "dummy"));
    }

    /** {@inheritDoc} */
    @Override protected void setEncryptionService(TcpDiscoveryS3IpFinder finder) {
        EncryptionService encryptionSvc = MockEncryptionService.instance();
        encryptionSvc.init();
        finder.setEncryptionService(encryptionSvc);
    }

    /** {@inheritDoc} */
    @Override protected TcpDiscoveryS3IpFinder ipFinder() {
        TcpDiscoveryS3IpFinder ipFinder = Mockito.spy(new TcpDiscoveryS3IpFinder());

        Mockito.doReturn(new DummyS3Client()).when(ipFinder).createAmazonS3Client();

        setAwsCredentials(ipFinder);
        setBucketName(ipFinder);
        setKeyPrefix(ipFinder);
        setEncryptionService(ipFinder);

        return ipFinder;
    }

    /** {@inheritDoc} */
    @Override public void testIpFinder() throws Exception {
        injectLogger(finder);

        assert finder.isShared() : "Ip finder should be shared by default.";

        super.testIpFinder();
    }
}
