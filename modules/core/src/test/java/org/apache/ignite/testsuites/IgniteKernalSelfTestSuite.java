/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.testsuites;

import org.apache.ignite.cache.NodeWithFilterRestartTest;
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
import org.apache.ignite.internal.processors.cluster.ChangeTopologyWatcherTest;
import org.apache.ignite.internal.processors.cluster.GridAddressResolverSelfTest;
import org.apache.ignite.internal.processors.cluster.GridUpdateNotifierSelfTest;
import org.apache.ignite.internal.processors.cluster.baseline.autoadjust.ChangeTopologyWatcher;
import org.apache.ignite.internal.processors.port.GridPortProcessorSelfTest;
import org.apache.ignite.internal.util.GridStartupWithUndefinedIgniteHomeSelfTest;
import org.apache.ignite.spi.communication.GridCacheMessageSelfTest;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Kernal self test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    GridGetOrStartSelfTest.class,
    GridSameVmStartupSelfTest.class,
    GridSpiExceptionSelfTest.class,
    GridRuntimeExceptionSelfTest.class,
    GridFailedInputParametersSelfTest.class,
    GridNodeFilterSelfTest.class,
    GridNodeVisorAttributesSelfTest.class,
    GridDiscoverySelfTest.class,
    GridCommunicationSelfTest.class,
    GridEventStorageManagerSelfTest.class,
    GridCommunicationSendMessageSelfTest.class,
    GridCacheMessageSelfTest.class,
    GridDeploymentManagerStopSelfTest.class,
    GridManagerStopSelfTest.class,
    GridDiscoveryManagerAttributesSelfTest.RegularDiscovery.class,
    GridDiscoveryManagerAttributesSelfTest.ClientDiscovery.class,
    GridDiscoveryManagerAliveCacheSelfTest.class,
    GridDiscoveryEventSelfTest.class,
    GridPortProcessorSelfTest.class,
    GridHomePathSelfTest.class,
    GridStartupWithUndefinedIgniteHomeSelfTest.class,
    GridVersionSelfTest.class,
    GridListenActorSelfTest.class,
    GridNodeLocalSelfTest.class,
    GridKernalConcurrentAccessStopSelfTest.class,
    IgniteConcurrentEntryProcessorAccessStopTest.class,
    GridUpdateNotifierSelfTest.class,
    GridAddressResolverSelfTest.class,
    ChangeTopologyWatcherTest.class,
    IgniteUpdateNotifierPerClusterSettingSelfTest.class,
    GridLocalEventListenerSelfTest.class,
    IgniteTopologyPrintFormatSelfTest.class,
    IgniteConnectionConcurrentReserveAndRemoveTest.class,
    LongJVMPauseDetectorTest.class,
    ClusterMetricsSelfTest.class,
    DeploymentRequestOfUnknownClassProcessingTest.class,
    NodeWithFilterRestartTest.class
})
public class IgniteKernalSelfTestSuite {
}
