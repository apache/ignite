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

package org.apache.ignite.testsuites;

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.spi.checkpoint.s3.S3CheckpointManagerSelfTest;
import org.apache.ignite.spi.checkpoint.s3.S3CheckpointSpiConfigSelfTest;
import org.apache.ignite.spi.checkpoint.s3.S3CheckpointSpiSelfTest;
import org.apache.ignite.spi.checkpoint.s3.S3CheckpointSpiStartStopBucketEndpointSelfTest;
import org.apache.ignite.spi.checkpoint.s3.S3CheckpointSpiStartStopSSEAlgorithmSelfTest;
import org.apache.ignite.spi.checkpoint.s3.S3CheckpointSpiStartStopSelfTest;
import org.apache.ignite.spi.checkpoint.s3.S3SessionCheckpointSelfTest;
import org.apache.ignite.spi.discovery.tcp.ipfinder.s3.TcpDiscoveryS3IpFinderAwsCredentialsProviderSelfTest;
import org.apache.ignite.spi.discovery.tcp.ipfinder.s3.TcpDiscoveryS3IpFinderAwsCredentialsSelfTest;
import org.apache.ignite.spi.discovery.tcp.ipfinder.s3.TcpDiscoveryS3IpFinderBucketEndpointSelfTest;
import org.apache.ignite.spi.discovery.tcp.ipfinder.s3.TcpDiscoveryS3IpFinderKeyPrefixSelfTest;
import org.apache.ignite.spi.discovery.tcp.ipfinder.s3.TcpDiscoveryS3IpFinderSSEAlgorithmSelfTest;
import org.apache.ignite.spi.discovery.tcp.ipfinder.s3.client.DummyObjectListingTest;
import org.apache.ignite.spi.discovery.tcp.ipfinder.s3.client.DummyS3ClientTest;
import org.apache.ignite.testframework.IgniteTestSuite;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * S3 integration tests.
 */
@RunWith(AllTests.class)
public class IgniteS3TestSuite {
    /**
     * @return Test suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new IgniteTestSuite("S3 Integration Test Suite");

        // Checkpoint SPI.
        suite.addTest(new JUnit4TestAdapter(S3CheckpointSpiConfigSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(S3CheckpointSpiSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(S3CheckpointSpiStartStopSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(S3CheckpointManagerSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(S3SessionCheckpointSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(S3CheckpointSpiStartStopBucketEndpointSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(S3CheckpointSpiStartStopSSEAlgorithmSelfTest.class));

        // S3 IP finder.
        suite.addTest(new JUnit4TestAdapter(DummyS3ClientTest.class));
        suite.addTest(new JUnit4TestAdapter(DummyObjectListingTest.class));
        suite.addTest(new JUnit4TestAdapter(TcpDiscoveryS3IpFinderAwsCredentialsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(TcpDiscoveryS3IpFinderAwsCredentialsProviderSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(TcpDiscoveryS3IpFinderBucketEndpointSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(TcpDiscoveryS3IpFinderSSEAlgorithmSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(TcpDiscoveryS3IpFinderKeyPrefixSelfTest.class));

        return suite;
    }

    /**
     * @return Access key.
     */
    public static String getAccessKey() {
        return getRequiredEnvVar("test.amazon.access.key");
    }

    /**
     * @return Access key.
     */
    public static String getSecretKey() {
        return getRequiredEnvVar("test.amazon.secret.key");
    }

    public static String getBucketName(final String defaultBucketName) {
        String value = System.getenv("test.s3.bucket.name");

        return value == null ? defaultBucketName : value;
    }

    private static String getRequiredEnvVar(String name) {
        String key = System.getenv(name);

        assert key != null : String.format("Environment variable '%s' is not set", name);

        return key;
    }
}
