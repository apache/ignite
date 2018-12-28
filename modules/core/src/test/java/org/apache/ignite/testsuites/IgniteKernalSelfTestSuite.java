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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.ClusterMetricsSelfTest;
import org.apache.ignite.internal.GridCommunicationSelfTest;
import org.apache.ignite.internal.GridDiscoveryEventSelfTest;
import org.apache.ignite.internal.GridDiscoverySelfTest;
import org.apache.ignite.internal.GridFailedInputParametersSelfTest;
import org.apache.ignite.internal.GridGetOrStartSelfTest;
import org.apache.ignite.internal.GridHomePathSelfTest;
import org.apache.ignite.internal.GridKernalConcurrentAccessStopSelfTest;
import org.apache.ignite.internal.GridListenActorSelfTest;
import org.apache.ignite.internal.GridLocalEventListenerSelfTest;
import org.apache.ignite.internal.GridNodeFilterSelfTest;
import org.apache.ignite.internal.GridNodeLocalSelfTest;
import org.apache.ignite.internal.GridNodeVisorAttributesSelfTest;
import org.apache.ignite.internal.GridRuntimeExceptionSelfTest;
import org.apache.ignite.internal.GridSameVmStartupSelfTest;
import org.apache.ignite.internal.GridSpiExceptionSelfTest;
import org.apache.ignite.internal.GridVersionSelfTest;
import org.apache.ignite.internal.IgniteConcurrentEntryProcessorAccessStopTest;
import org.apache.ignite.internal.IgniteConnectionConcurrentReserveAndRemoveTest;
import org.apache.ignite.internal.IgniteUpdateNotifierPerClusterSettingSelfTest;
import org.apache.ignite.internal.LongJVMPauseDetectorTest;
import org.apache.ignite.internal.managers.GridManagerStopSelfTest;
import org.apache.ignite.internal.managers.communication.GridCommunicationSendMessageSelfTest;
import org.apache.ignite.internal.managers.deployment.DeploymentRequestOfUnknownClassProcessingTest;
import org.apache.ignite.internal.managers.deployment.GridDeploymentManagerStopSelfTest;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManagerAliveCacheSelfTest;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManagerAttributesSelfTest;
import org.apache.ignite.internal.managers.discovery.IgniteTopologyPrintFormatSelfTest;
import org.apache.ignite.internal.managers.events.GridEventStorageManagerSelfTest;
import org.apache.ignite.internal.processors.cluster.GridAddressResolverSelfTest;
import org.apache.ignite.internal.processors.cluster.GridUpdateNotifierSelfTest;
import org.apache.ignite.internal.processors.port.GridPortProcessorSelfTest;
import org.apache.ignite.internal.util.GridStartupWithUndefinedIgniteHomeSelfTest;
import org.apache.ignite.spi.communication.GridCacheMessageSelfTest;
import org.apache.ignite.testframework.GridTestUtils;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;

/**
 * Kernal self test suite.
 */
@RunWith(IgniteKernalSelfTestSuite.DynamicSuite.class)
public class IgniteKernalSelfTestSuite {
    /**
     * @return Kernal test suite.
     */
    public static List<Class<?>> suite() {
        return suite(null);
    }

    /**
     * @param ignoredTests Tests to ignore.
     * @return Test suite.
     */
    public static List<Class<?>> suite(Collection<Class> ignoredTests) {
        List<Class<?>> suite = new ArrayList<>();

        suite.add(GridGetOrStartSelfTest.class);
        suite.add(GridSameVmStartupSelfTest.class);
        suite.add(GridSpiExceptionSelfTest.class);
        suite.add(GridRuntimeExceptionSelfTest.class);
        suite.add(GridFailedInputParametersSelfTest.class);
        suite.add(GridNodeFilterSelfTest.class);
        suite.add(GridNodeVisorAttributesSelfTest.class);
        suite.add(GridDiscoverySelfTest.class);
        suite.add(GridCommunicationSelfTest.class);
        suite.add(GridEventStorageManagerSelfTest.class);
        suite.add(GridCommunicationSendMessageSelfTest.class);
        suite.add(GridCacheMessageSelfTest.class);
        suite.add(GridDeploymentManagerStopSelfTest.class);
        suite.add(GridManagerStopSelfTest.class);
        suite.add(GridDiscoveryManagerAttributesSelfTest.RegularDiscovery.class);
        suite.add(GridDiscoveryManagerAttributesSelfTest.ClientDiscovery.class);
        suite.add(GridDiscoveryManagerAliveCacheSelfTest.class);
        suite.add(GridDiscoveryEventSelfTest.class);
        suite.add(GridPortProcessorSelfTest.class);
        suite.add(GridHomePathSelfTest.class);
        suite.add(GridStartupWithUndefinedIgniteHomeSelfTest.class);
        GridTestUtils.addTestIfNeeded(suite, GridVersionSelfTest.class, ignoredTests);
        suite.add(GridListenActorSelfTest.class);
        suite.add(GridNodeLocalSelfTest.class);
        suite.add(GridKernalConcurrentAccessStopSelfTest.class);
        suite.add(IgniteConcurrentEntryProcessorAccessStopTest.class);
        suite.add(GridUpdateNotifierSelfTest.class);
        suite.add(GridAddressResolverSelfTest.class);
        suite.add(IgniteUpdateNotifierPerClusterSettingSelfTest.class);
        suite.add(GridLocalEventListenerSelfTest.class);
        suite.add(IgniteTopologyPrintFormatSelfTest.class);
        suite.add(IgniteConnectionConcurrentReserveAndRemoveTest.class);
        suite.add(LongJVMPauseDetectorTest.class);
        suite.add(ClusterMetricsSelfTest.class);
        suite.add(DeploymentRequestOfUnknownClassProcessingTest.class);

        return suite;
    }

    /** */
    public static class DynamicSuite extends Suite {
        /** */
        public DynamicSuite(Class<?> cls) throws InitializationError {
            super(cls, suite().toArray(new Class<?>[] {null}));
        }
    }
}
