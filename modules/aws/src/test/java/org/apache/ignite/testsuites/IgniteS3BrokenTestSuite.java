/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.testsuites;

import junit.framework.TestSuite;
import org.apache.ignite.spi.discovery.tcp.ipfinder.s3.TcpDiscoveryS3IpFinderAwsCredentialsProviderSelfTest;
import org.apache.ignite.spi.discovery.tcp.ipfinder.s3.TcpDiscoveryS3IpFinderAwsCredentialsSelfTest;
import org.apache.ignite.testframework.IgniteTestSuite;

/**
 *
 */
public class IgniteS3BrokenTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new IgniteTestSuite("S3 Integration Broken Test Suite");

        // S3 IP finder.
        suite.addTestSuite(TcpDiscoveryS3IpFinderAwsCredentialsSelfTest.class);
        suite.addTestSuite(TcpDiscoveryS3IpFinderAwsCredentialsProviderSelfTest.class);

        return suite;
    }
}