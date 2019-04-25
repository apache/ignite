/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.testsuites;

import org.apache.ignite.internal.IgniteClientConnectAfterCommunicationFailureTest;
import org.apache.ignite.internal.IgniteClientReconnectApiExceptionTest;
import org.apache.ignite.internal.IgniteClientReconnectAtomicsTest;
import org.apache.ignite.internal.IgniteClientReconnectAtomicsWithLostPartitionsTest;
import org.apache.ignite.internal.IgniteClientReconnectBinaryContexTest;
import org.apache.ignite.internal.IgniteClientReconnectCacheTest;
import org.apache.ignite.internal.IgniteClientReconnectCollectionsTest;
import org.apache.ignite.internal.IgniteClientReconnectComputeTest;
import org.apache.ignite.internal.IgniteClientReconnectContinuousProcessorTest;
import org.apache.ignite.internal.IgniteClientReconnectDelayedSpiTest;
import org.apache.ignite.internal.IgniteClientReconnectDiscoveryStateTest;
import org.apache.ignite.internal.IgniteClientReconnectFailoverTest;
import org.apache.ignite.internal.IgniteClientReconnectServicesTest;
import org.apache.ignite.internal.IgniteClientReconnectStopTest;
import org.apache.ignite.internal.IgniteClientReconnectStreamerTest;
import org.apache.ignite.internal.IgniteClientRejoinTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/** */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    IgniteClientConnectAfterCommunicationFailureTest.class,
    IgniteClientReconnectStopTest.class,
    IgniteClientReconnectApiExceptionTest.class,
    IgniteClientReconnectDiscoveryStateTest.class,
    IgniteClientReconnectCacheTest.class,
    IgniteClientReconnectDelayedSpiTest.class,
    IgniteClientReconnectBinaryContexTest.class,
    IgniteClientReconnectContinuousProcessorTest.class,
    IgniteClientReconnectComputeTest.class,
    IgniteClientReconnectAtomicsTest.class,
    IgniteClientReconnectAtomicsWithLostPartitionsTest.class,
    IgniteClientReconnectCollectionsTest.class,
    IgniteClientReconnectServicesTest.class,
    IgniteClientReconnectStreamerTest.class,
    IgniteClientReconnectFailoverTest.class,
    IgniteClientRejoinTest.class
})
public class IgniteClientReconnectTestSuite {
}
