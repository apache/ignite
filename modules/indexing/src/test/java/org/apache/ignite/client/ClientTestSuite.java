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

package org.apache.ignite.client;

import org.apache.ignite.internal.client.thin.AtomicLongTest;
import org.apache.ignite.internal.client.thin.CacheAsyncTest;
import org.apache.ignite.internal.client.thin.CacheEntryListenersTest;
import org.apache.ignite.internal.client.thin.ClusterApiTest;
import org.apache.ignite.internal.client.thin.ClusterGroupTest;
import org.apache.ignite.internal.client.thin.ComputeTaskTest;
import org.apache.ignite.internal.client.thin.DataReplicationOperationsTest;
import org.apache.ignite.internal.client.thin.FunctionalTest;
import org.apache.ignite.internal.client.thin.IgniteSetTest;
import org.apache.ignite.internal.client.thin.MetadataRegistrationTest;
import org.apache.ignite.internal.client.thin.OptimizedMarshallerClassesCachedTest;
import org.apache.ignite.internal.client.thin.ReliableChannelTest;
import org.apache.ignite.internal.client.thin.ServicesBinaryArraysTests;
import org.apache.ignite.internal.client.thin.ServicesTest;
import org.apache.ignite.internal.client.thin.ThinClientEnpointsDiscoveryTest;
import org.apache.ignite.internal.client.thin.ThinClientPartitionAwarenessBalancingTest;
import org.apache.ignite.internal.client.thin.ThinClientPartitionAwarenessDiscoveryTest;
import org.apache.ignite.internal.client.thin.ThinClientPartitionAwarenessResourceReleaseTest;
import org.apache.ignite.internal.client.thin.ThinClientPartitionAwarenessStableTopologyTest;
import org.apache.ignite.internal.client.thin.ThinClientPartitionAwarenessUnstableTopologyTest;
import org.apache.ignite.internal.client.thin.TimeoutTest;
import org.apache.ignite.internal.client.thin.events.IgniteClientConnectionEventListenerTest;
import org.apache.ignite.internal.client.thin.events.IgniteClientRequestEventListenerTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Tests for Java thin client.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    ClientConfigurationTest.class,
    ClientCacheConfigurationTest.class,
    FunctionalTest.class,
    IgniteBinaryTest.class,
    LoadTest.class,
    ReliabilityTest.class,
    SecurityTest.class,
    FunctionalQueryTest.class,
    IgniteBinaryQueryTest.class,
    SslParametersTest.class,
    ConnectionTest.class,
    ConnectToStartingNodeTest.class,
    AsyncChannelTest.class,
    ComputeTaskTest.class,
    ClusterApiTest.class,
    ClusterGroupTest.class,
    ServicesTest.class,
    ServicesBinaryArraysTests.class,
    CacheEntryListenersTest.class,
    ThinClientPartitionAwarenessStableTopologyTest.class,
    ThinClientPartitionAwarenessUnstableTopologyTest.class,
    ThinClientPartitionAwarenessResourceReleaseTest.class,
    ThinClientPartitionAwarenessDiscoveryTest.class,
    ThinClientPartitionAwarenessBalancingTest.class,
    ReliableChannelTest.class,
    CacheAsyncTest.class,
    TimeoutTest.class,
    OptimizedMarshallerClassesCachedTest.class,
    AtomicLongTest.class,
    BinaryConfigurationTest.class,
    IgniteSetTest.class,
    DataReplicationOperationsTest.class,
    MetadataRegistrationTest.class,
    IgniteClientConnectionEventListenerTest.class,
    IgniteClientRequestEventListenerTest.class,
    ThinClientEnpointsDiscoveryTest.class,
})
public class ClientTestSuite {
    // No-op.
}
