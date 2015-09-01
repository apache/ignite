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

import junit.framework.TestSuite;
import org.apache.ignite.spi.checkpoint.s3.S3CheckpointManagerSelfTest;
import org.apache.ignite.spi.checkpoint.s3.S3CheckpointSpiConfigSelfTest;
import org.apache.ignite.spi.checkpoint.s3.S3CheckpointSpiSelfTest;
import org.apache.ignite.spi.checkpoint.s3.S3CheckpointSpiStartStopSelfTest;
import org.apache.ignite.spi.checkpoint.s3.S3SessionCheckpointSelfTest;
import org.apache.ignite.spi.discovery.tcp.ipfinder.s3.TcpDiscoveryS3IpFinderSelfTest;

/**
 * S3 integration tests.
 */
public class IgniteS3TestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("S3 Integration Test Suite");

        // Checkpoint SPI.
        suite.addTest(new TestSuite(S3CheckpointSpiConfigSelfTest.class));
        suite.addTest(new TestSuite(S3CheckpointSpiSelfTest.class));
        suite.addTest(new TestSuite(S3CheckpointSpiStartStopSelfTest.class));
        suite.addTest(new TestSuite(S3CheckpointManagerSelfTest.class));
        suite.addTest(new TestSuite(S3SessionCheckpointSelfTest.class));

        // S3 IP finder.
        suite.addTest(new TestSuite(TcpDiscoveryS3IpFinderSelfTest.class));

        return suite;
    }

    /**
     * @return Access key.
     */
    public static String getAccessKey() {
        String key = System.getenv("test.amazon.access.key");

        assert key != null : "Environment variable 'test.amazon.access.key' is not set";

        return key;
    }

    /**
     * @return Access key.
     */
    public static String getSecretKey() {
        String key = System.getenv("test.amazon.secret.key");

        assert key != null : "Environment variable 'test.amazon.secret.key' is not set";

        return key;
    }
}