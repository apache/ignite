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
import org.apache.ignite.internal.processors.security.cache.CacheOperationPermissionCheckTest;
import org.apache.ignite.internal.processors.security.cache.EntryProcessorPermissionCheckTest;
import org.apache.ignite.internal.processors.security.cache.ScanQueryPermissionCheckTest;
import org.apache.ignite.internal.processors.security.cache.closure.CacheLoadRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.processors.security.cache.closure.EntryProcessorRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.processors.security.cache.closure.ScanQueryRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.processors.security.client.ThinClientPermissionCheckTest;
import org.apache.ignite.internal.processors.security.compute.ComputePermissionCheckTest;
import org.apache.ignite.internal.processors.security.compute.closure.ComputeTaskRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.processors.security.compute.closure.DistributedClosureRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.processors.security.compute.closure.ExecutorServiceRemoteSecurityContextCheckTest;
import org.apache.ignite.internal.processors.security.datastreamer.DataStreamerPermissionCheckTest;
import org.apache.ignite.internal.processors.security.datastreamer.closure.DataStreamerRemoteSecurityContextCheckTest;
import org.junit.BeforeClass;

import static org.apache.ignite.internal.IgniteFeatures.IGNITE_SECURITY_PROCESSOR;

/**
 * Security test suite.
 */
public class SecurityTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite security suite.");

        System.setProperty(IGNITE_SECURITY_PROCESSOR.name(), "true");

        suite.addTestSuite(CacheOperationPermissionCheckTest.class);
        suite.addTestSuite(DataStreamerPermissionCheckTest.class);
        suite.addTestSuite(ScanQueryPermissionCheckTest.class);
        suite.addTestSuite(EntryProcessorPermissionCheckTest.class);
        suite.addTestSuite(ComputePermissionCheckTest.class);

        suite.addTestSuite(DistributedClosureRemoteSecurityContextCheckTest.class);
        suite.addTestSuite(ComputeTaskRemoteSecurityContextCheckTest.class);
        suite.addTestSuite(ExecutorServiceRemoteSecurityContextCheckTest.class);
        suite.addTestSuite(ScanQueryRemoteSecurityContextCheckTest.class);
        suite.addTestSuite(EntryProcessorRemoteSecurityContextCheckTest.class);
        suite.addTestSuite(DataStreamerRemoteSecurityContextCheckTest.class);
        suite.addTestSuite(CacheLoadRemoteSecurityContextCheckTest.class);
        suite.addTestSuite(ThinClientPermissionCheckTest.class);

        return suite;
    }
}
