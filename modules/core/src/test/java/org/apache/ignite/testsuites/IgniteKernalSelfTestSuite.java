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

import junit.framework.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.managers.*;
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.internal.managers.deployment.*;
import org.apache.ignite.internal.managers.discovery.*;
import org.apache.ignite.internal.managers.events.*;
import org.apache.ignite.internal.managers.swapspace.*;
import org.apache.ignite.internal.processors.port.*;
import org.apache.ignite.internal.processors.service.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.spi.communication.*;
import org.apache.ignite.testframework.*;

import java.util.*;

/**
 * Kernal self test suite.
 */
public class IgniteKernalSelfTestSuite extends TestSuite {
    /**
     * @return Kernal test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        return suite(null);
    }

    /**
     * @param ignoredTests Tests don't include in the execution.
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite(Set<Class> ignoredTests) throws Exception {
        TestSuite suite = new TestSuite("Ignite Kernal Test Suite");

        suite.addTestSuite(GridSameVmStartupSelfTest.class);
        suite.addTestSuite(GridSpiExceptionSelfTest.class);
        suite.addTestSuite(GridRuntimeExceptionSelfTest.class);
        suite.addTestSuite(GridFailedInputParametersSelfTest.class);
        suite.addTestSuite(GridNodeFilterSelfTest.class);
        suite.addTestSuite(GridNodeVisorAttributesSelfTest.class);
        suite.addTestSuite(GridDiscoverySelfTest.class);
        suite.addTestSuite(GridCommunicationSelfTest.class);
        suite.addTestSuite(GridEventStorageManagerSelfTest.class);
        suite.addTestSuite(GridSwapSpaceManagerSelfTest.class);
        suite.addTestSuite(GridCommunicationSendMessageSelfTest.class);
        suite.addTestSuite(GridCacheMessageSelfTest.class);
        suite.addTestSuite(GridDeploymentManagerStopSelfTest.class);
        suite.addTestSuite(GridManagerStopSelfTest.class);
        suite.addTestSuite(GridDiscoveryManagerAttributesSelfTest.RegularDiscovery.class);
        suite.addTestSuite(GridDiscoveryManagerAttributesSelfTest.ClientDiscovery.class);
        suite.addTestSuite(GridDiscoveryManagerAliveCacheSelfTest.class);
        suite.addTestSuite(GridDiscoveryManagerSelfTest.RegularDiscovery.class);
        suite.addTestSuite(GridDiscoveryManagerSelfTest.ClientDiscovery.class);
        suite.addTestSuite(GridDiscoveryEventSelfTest.class);
        suite.addTestSuite(GridPortProcessorSelfTest.class);
        suite.addTestSuite(GridHomePathSelfTest.class);
        GridTestUtils.addTestIfNeeded(suite, GridStartupWithSpecifiedWorkDirectorySelfTest.class, ignoredTests);
        suite.addTestSuite(GridStartupWithUndefinedIgniteHomeSelfTest.class);
        GridTestUtils.addTestIfNeeded(suite, GridVersionSelfTest.class, ignoredTests);
        suite.addTestSuite(GridListenActorSelfTest.class);
        suite.addTestSuite(GridNodeLocalSelfTest.class);
        suite.addTestSuite(GridKernalConcurrentAccessStopSelfTest.class);
        suite.addTestSuite(GridUpdateNotifierSelfTest.class);
        suite.addTestSuite(GridLocalEventListenerSelfTest.class);

        // Managed Services.
        suite.addTestSuite(GridServiceProcessorSingleNodeSelfTest.class);
        suite.addTestSuite(GridServiceProcessorMultiNodeSelfTest.class);
        suite.addTestSuite(GridServiceProcessorMultiNodeConfigSelfTest.class);
        suite.addTestSuite(GridServiceProcessorProxySelfTest.class);
        suite.addTestSuite(GridServiceReassignmentSelfTest.class);
        suite.addTestSuite(GridServiceClientNodeTest.class);

        return suite;
    }
}
