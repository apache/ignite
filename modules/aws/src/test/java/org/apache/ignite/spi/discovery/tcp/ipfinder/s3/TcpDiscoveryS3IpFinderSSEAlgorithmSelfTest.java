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
import org.apache.ignite.testsuites.IgniteS3TestSuite;
import org.junit.Test;

/**
 * TcpDiscoveryS3IpFinder tests server-side encryption algorithm for Amazon S3-managed encryption keys.
 * For information about possible S3-managed encryption keys visit
 * <a href="http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingServerSideEncryption.html">docs.aws.amazon.com</a>.
 */
public class TcpDiscoveryS3IpFinderSSEAlgorithmSelfTest extends TcpDiscoveryS3IpFinderAbstractSelfTest {
    /**
     * Constructor.
     *
     * @throws Exception If any error occurs.
     */
    public TcpDiscoveryS3IpFinderSSEAlgorithmSelfTest() throws Exception {
        SSEAlgorithm = "AES256";
    }

    /** {@inheritDoc} */
    @Override protected void setAwsCredentials(TcpDiscoveryS3IpFinder finder) {
        finder.setAwsCredentials(new BasicAWSCredentials(IgniteS3TestSuite.getAccessKey(),
            IgniteS3TestSuite.getSecretKey()));
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testIpFinder() throws Exception {
        super.testIpFinder();
    }
}
