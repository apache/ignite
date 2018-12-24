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

import java.util.Set;
import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.failure.FailureHandlingConfigurationTest;
import org.apache.ignite.failure.IoomFailureHandlerTest;
import org.apache.ignite.failure.SystemWorkersTerminationTest;
import org.apache.ignite.internal.ClusterBaselineNodesMetricsSelfTest;
import org.apache.ignite.internal.encryption.EncryptedCacheBigEntryTest;
import org.apache.ignite.internal.encryption.EncryptedCacheCreateTest;
import org.apache.ignite.internal.encryption.EncryptedCacheDestroyTest;
import org.apache.ignite.internal.encryption.EncryptedCacheGroupCreateTest;
import org.apache.ignite.internal.encryption.EncryptedCacheNodeJoinTest;
import org.apache.ignite.internal.encryption.EncryptedCachePreconfiguredRestartTest;
import org.apache.ignite.internal.encryption.EncryptedCacheRestartTest;
import org.apache.ignite.internal.GridNodeMetricsLogPdsSelfTest;
import org.apache.ignite.internal.processors.service.ServiceDeploymentOnActivationTest;
import org.apache.ignite.internal.processors.service.ServiceDeploymentOutsideBaselineTest;
import org.apache.ignite.marshaller.GridMarshallerMappingConsistencyTest;
import org.apache.ignite.util.GridCommandHandlerSslTest;
import org.apache.ignite.util.GridCommandHandlerTest;
import org.apache.ignite.util.GridInternalTaskUnusedWalSegmentsTest;
import org.jetbrains.annotations.Nullable;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Basic test suite.
 */
@RunWith(AllTests.class)
public class IgniteBasicWithPersistenceTestSuite {
    /**
     * @return Test suite.
     */
    public static TestSuite suite() {
        return suite(null);
    }

    /**
     * @param ignoredTests Tests don't include in the execution. Providing null means nothing to exclude.
     * @return Test suite.
     */
    public static TestSuite suite(@Nullable final Set<Class> ignoredTests) {
        TestSuite suite = new TestSuite("Ignite Basic With Persistence Test Suite");

        suite.addTest(new JUnit4TestAdapter(IoomFailureHandlerTest.class));
        suite.addTest(new JUnit4TestAdapter(ClusterBaselineNodesMetricsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(ServiceDeploymentOnActivationTest.class));
        suite.addTest(new JUnit4TestAdapter(ServiceDeploymentOutsideBaselineTest.class));
        suite.addTest(new JUnit4TestAdapter(GridMarshallerMappingConsistencyTest.class));
        suite.addTest(new JUnit4TestAdapter(SystemWorkersTerminationTest.class));
        suite.addTest(new JUnit4TestAdapter(FailureHandlingConfigurationTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCommandHandlerTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCommandHandlerSslTest.class));
        suite.addTest(new JUnit4TestAdapter(GridInternalTaskUnusedWalSegmentsTest.class));

        suite.addTest(new JUnit4TestAdapter(GridNodeMetricsLogPdsSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(EncryptedCacheBigEntryTest.class));
        suite.addTest(new JUnit4TestAdapter(EncryptedCacheCreateTest.class));
        suite.addTest(new JUnit4TestAdapter(EncryptedCacheDestroyTest.class));
        suite.addTest(new JUnit4TestAdapter(EncryptedCacheGroupCreateTest.class));
        suite.addTest(new JUnit4TestAdapter(EncryptedCacheNodeJoinTest.class));
        suite.addTest(new JUnit4TestAdapter(EncryptedCacheRestartTest.class));
        suite.addTest(new JUnit4TestAdapter(EncryptedCachePreconfiguredRestartTest.class));

        return suite;
    }
}
