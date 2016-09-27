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
import org.apache.ignite.internal.websession.WebSessionReplicatedSelfTest;
import org.apache.ignite.internal.websession.WebSessionReplicatedV1SelfTest;
import org.apache.ignite.internal.websession.WebSessionSelfTest;
import org.apache.ignite.internal.websession.WebSessionTransactionalSelfTest;
import org.apache.ignite.internal.websession.WebSessionTransactionalV1SelfTest;
import org.apache.ignite.internal.websession.WebSessionV1SelfTest;
import org.apache.ignite.spi.checkpoint.s3.S3CheckpointManagerSelfTest;
import org.apache.ignite.spi.checkpoint.s3.S3CheckpointSpiSelfTest;
import org.apache.ignite.spi.checkpoint.s3.S3CheckpointSpiStartStopSelfTest;
import org.apache.ignite.spi.checkpoint.s3.S3SessionCheckpointSelfTest;
import org.apache.ignite.spi.discovery.tcp.ipfinder.s3.TcpDiscoveryS3IpFinderSelfTest;
import org.apache.ignite.testframework.IgniteTestSuite;

/**
 * Special test suite with ignored tests.
 */
public class IgniteIgnoredTestSuite extends TestSuite {
    /**
     * @return IgniteCache test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        IgniteTestSuite suite = new IgniteTestSuite(null, "Ignite Ignored Test Suite", true);

        /* --- AWS --- */
        suite.addTestSuite(S3CheckpointManagerSelfTest.class);
        suite.addTestSuite(S3CheckpointSpiSelfTest.class);
        suite.addTestSuite(S3CheckpointSpiStartStopSelfTest.class);
        suite.addTestSuite(S3SessionCheckpointSelfTest.class);
        suite.addTestSuite(TcpDiscoveryS3IpFinderSelfTest.class);

        /* --- WEB SESSIONS --- */
        suite.addTestSuite(WebSessionSelfTest.class);
        suite.addTestSuite(WebSessionTransactionalSelfTest.class);
        suite.addTestSuite(WebSessionReplicatedSelfTest.class);
        suite.addTestSuite(WebSessionV1SelfTest.class);
        suite.addTestSuite(WebSessionTransactionalV1SelfTest.class);
        suite.addTestSuite(WebSessionReplicatedV1SelfTest.class);

        return suite;
    }
}
