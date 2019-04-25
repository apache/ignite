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
import org.apache.ignite.testsuites.IgniteS3TestSuite;
import org.junit.Test;

/**
 * TcpDiscoveryS3IpFinder tests bucket endpoint for IP finder.
 * For information about possible endpoint names visit
 * <a href="http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region">docs.aws.amazon.com</a>.
 */
public class TcpDiscoveryS3IpFinderBucketEndpointSelfTest extends TcpDiscoveryS3IpFinderAbstractSelfTest {
    /**
     * Constructor.
     *
     * @throws Exception If any error occurs.
     */
    public TcpDiscoveryS3IpFinderBucketEndpointSelfTest() throws Exception {
        bucketEndpoint = "s3.us-east-2.amazonaws.com";
    }

    /** {@inheritDoc} */
    @Override protected void setAwsCredentials(TcpDiscoveryS3IpFinder finder) {
        finder.setAwsCredentials(new BasicAWSCredentials(IgniteS3TestSuite.getAccessKey(),
            IgniteS3TestSuite.getSecretKey()));
    }

    /** {@inheritDoc} */
    @Override protected void setBucketName(TcpDiscoveryS3IpFinder finder) {
        super.setBucketName(finder);

        finder.setBucketName(getBucketName() + "-e");
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testIpFinder() throws Exception {
        super.testIpFinder();
    }
}
