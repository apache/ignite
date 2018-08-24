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
import junit.framework.TestSuite;
import org.apache.ignite.failure.IoomFailureHandlerTest;
import org.apache.ignite.failure.SystemWorkersTerminationTest;
import org.apache.ignite.internal.ClusterBaselineNodesMetricsSelfTest;
import org.apache.ignite.internal.processors.service.ServiceDeploymentOnActivationTest;
import org.apache.ignite.internal.processors.service.ServiceDeploymentOutsideBaselineTest;
import org.apache.ignite.marshaller.GridMarshallerMappingConsistencyTest;
import org.apache.ignite.util.GridCommandHandlerTest;
import org.apache.ignite.util.GridInternalTaskUnusedWalSegmentsTest;
import org.jetbrains.annotations.Nullable;

/**
 * Basic test suite.
 */
public class IgniteBasicWithPersistenceTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        return suite(null);
    }

    /**
     * @param ignoredTests Tests don't include in the execution. Providing null means nothing to exclude.
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite(@Nullable final Set<Class> ignoredTests) throws Exception {
        TestSuite suite = new TestSuite("Ignite Basic With Persistence Test Suite");

        suite.addTestSuite(IoomFailureHandlerTest.class);
        suite.addTestSuite(ClusterBaselineNodesMetricsSelfTest.class);
        suite.addTestSuite(ServiceDeploymentOnActivationTest.class);
        suite.addTestSuite(ServiceDeploymentOutsideBaselineTest.class);
        suite.addTestSuite(GridMarshallerMappingConsistencyTest.class);
        suite.addTestSuite(SystemWorkersTerminationTest.class);

        suite.addTestSuite(GridCommandHandlerTest.class);
        suite.addTestSuite(GridInternalTaskUnusedWalSegmentsTest.class);

        return suite;
    }
}
