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

import org.apache.ignite.examples.BasicExamplesMultiNodeSelfTest;
import org.apache.ignite.examples.BasicExamplesSelfTest;
import org.apache.ignite.examples.CacheClientBinaryExampleTest;
import org.apache.ignite.examples.CacheContinuousQueryExamplesSelfTest;
import org.apache.ignite.examples.CacheExamplesMultiNodeSelfTest;
import org.apache.ignite.examples.CacheExamplesSelfTest;
import org.apache.ignite.examples.CheckpointExamplesSelfTest;
import org.apache.ignite.examples.ClusterGroupExampleSelfTest;
import org.apache.ignite.examples.ComputeClientBinaryExampleTest;
import org.apache.ignite.examples.ContinuationExamplesMultiNodeSelfTest;
import org.apache.ignite.examples.ContinuationExamplesSelfTest;
import org.apache.ignite.examples.ContinuousMapperExamplesMultiNodeSelfTest;
import org.apache.ignite.examples.ContinuousMapperExamplesSelfTest;
import org.apache.ignite.examples.DeploymentExamplesMultiNodeSelfTest;
import org.apache.ignite.examples.DeploymentExamplesSelfTest;
import org.apache.ignite.examples.EncryptedCacheExampleSelfTest;
import org.apache.ignite.examples.EventsExamplesMultiNodeSelfTest;
import org.apache.ignite.examples.EventsExamplesSelfTest;
import org.apache.ignite.examples.LifecycleExamplesSelfTest;
import org.apache.ignite.examples.MemcacheRestExamplesMultiNodeSelfTest;
import org.apache.ignite.examples.MemcacheRestExamplesSelfTest;
import org.apache.ignite.examples.MessagingExamplesSelfTest;
import org.apache.ignite.examples.MonteCarloExamplesMultiNodeSelfTest;
import org.apache.ignite.examples.MonteCarloExamplesSelfTest;
import org.apache.ignite.examples.OpenCensusMetricsExporterExampleSelfTest;
import org.apache.ignite.examples.SpringBeanExamplesSelfTest;
import org.apache.ignite.examples.SpringDataExampleSelfTest;
import org.apache.ignite.examples.SqlExamplesSelfTest;
import org.apache.ignite.examples.TaskExamplesMultiNodeSelfTest;
import org.apache.ignite.examples.TaskExamplesSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Examples test suite.
 * <p>
 * Contains all Ignite examples tests.</p>
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    CacheExamplesSelfTest.class,
    SqlExamplesSelfTest.class,
    BasicExamplesSelfTest.class,
    ContinuationExamplesSelfTest.class,
    ContinuousMapperExamplesSelfTest.class,
    DeploymentExamplesSelfTest.class,
    EventsExamplesSelfTest.class,
    LifecycleExamplesSelfTest.class,
    MessagingExamplesSelfTest.class,
    MemcacheRestExamplesSelfTest.class,
    MonteCarloExamplesSelfTest.class,
    TaskExamplesSelfTest.class,
    SpringBeanExamplesSelfTest.class,
    SpringDataExampleSelfTest.class,
    CheckpointExamplesSelfTest.class,
    ClusterGroupExampleSelfTest.class,
    CacheContinuousQueryExamplesSelfTest.class,

    // Multi-node.
    CacheExamplesMultiNodeSelfTest.class,
    BasicExamplesMultiNodeSelfTest.class,
    ContinuationExamplesMultiNodeSelfTest.class,
    ContinuousMapperExamplesMultiNodeSelfTest.class,
    DeploymentExamplesMultiNodeSelfTest.class,
    EventsExamplesMultiNodeSelfTest.class,
    TaskExamplesMultiNodeSelfTest.class,
    MemcacheRestExamplesMultiNodeSelfTest.class,
    MonteCarloExamplesMultiNodeSelfTest.class,

    // Binary.
    CacheClientBinaryExampleTest.class,
    ComputeClientBinaryExampleTest.class,

    // ML Grid.
    IgniteExamplesMLTestSuite.class,

    // Encryption.
    EncryptedCacheExampleSelfTest.class,

    // OpenCensus.
    OpenCensusMetricsExporterExampleSelfTest.class
})
public class IgniteExamplesSelfTestSuite {
}
