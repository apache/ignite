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

import org.apache.ignite.internal.managers.deployment.GridDeploymentMessageCountSelfTest;
import org.apache.ignite.p2p.DeploymentClassLoaderCallableTest;
import org.apache.ignite.p2p.GridP2PClassLoadingSelfTest;
import org.apache.ignite.p2p.GridP2PContinuousDeploymentSelfTest;
import org.apache.ignite.p2p.GridP2PDifferentClassLoaderSelfTest;
import org.apache.ignite.p2p.GridP2PDoubleDeploymentSelfTest;
import org.apache.ignite.p2p.GridP2PHotRedeploymentSelfTest;
import org.apache.ignite.p2p.GridP2PJobClassLoaderSelfTest;
import org.apache.ignite.p2p.GridP2PLocalDeploymentSelfTest;
import org.apache.ignite.p2p.GridP2PMissedResourceCacheSizeSelfTest;
import org.apache.ignite.p2p.GridP2PNodeLeftSelfTest;
import org.apache.ignite.p2p.GridP2PRecursionTaskSelfTest;
import org.apache.ignite.p2p.GridP2PRemoteClassLoadersSelfTest;
import org.apache.ignite.p2p.GridP2PSameClassLoaderSelfTest;
import org.apache.ignite.p2p.GridP2PTimeoutSelfTest;
import org.apache.ignite.p2p.GridP2PUndeploySelfTest;
import org.apache.ignite.p2p.P2PScanQueryUndeployTest;
import org.apache.ignite.p2p.P2PStreamingClassLoaderTest;
import org.apache.ignite.p2p.SharedDeploymentTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * P2P test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    GridP2PDoubleDeploymentSelfTest.class,
    GridP2PHotRedeploymentSelfTest.class,
    GridP2PClassLoadingSelfTest.class,
    GridP2PUndeploySelfTest.class,
    GridP2PRemoteClassLoadersSelfTest.class,
    GridP2PNodeLeftSelfTest.class,
    GridP2PDifferentClassLoaderSelfTest.class,
    GridP2PSameClassLoaderSelfTest.class,
    GridP2PJobClassLoaderSelfTest.class,
    GridP2PRecursionTaskSelfTest.class,
    GridP2PLocalDeploymentSelfTest.class,
    //GridP2PTestTaskExecutionTest.class,
    GridP2PTimeoutSelfTest.class,
    GridP2PMissedResourceCacheSizeSelfTest.class,
    GridP2PContinuousDeploymentSelfTest.class,
    DeploymentClassLoaderCallableTest.class,
    P2PStreamingClassLoaderTest.class,
    SharedDeploymentTest.class,
    P2PScanQueryUndeployTest.class,
    GridDeploymentMessageCountSelfTest.class,
})
public class IgniteP2PSelfTestSuite {
}
