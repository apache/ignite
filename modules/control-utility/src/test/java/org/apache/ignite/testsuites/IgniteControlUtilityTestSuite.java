/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.testsuites;

import org.apache.ignite.events.BaselineEventsLocalTest;
import org.apache.ignite.events.BaselineEventsRemoteTest;
import org.apache.ignite.internal.commandline.CommandHandlerParsingTest;
import org.apache.ignite.internal.commandline.indexreader.IgniteIndexReaderTest;
import org.apache.ignite.internal.processors.security.GridCommandHandlerSslWithSecurityTest;
import org.apache.ignite.util.GridCommandHandlerBrokenIndexTest;
import org.apache.ignite.util.GridCommandHandlerCheckIndexesInlineSizeTest;
import org.apache.ignite.util.GridCommandHandlerClusterByClassTest;
import org.apache.ignite.util.GridCommandHandlerClusterByClassWithSSLTest;
import org.apache.ignite.util.GridCommandHandlerDefragmentationTest;
import org.apache.ignite.util.GridCommandHandlerIndexForceRebuildTest;
import org.apache.ignite.util.GridCommandHandlerIndexListTest;
import org.apache.ignite.util.GridCommandHandlerIndexRebuildStatusTest;
import org.apache.ignite.util.GridCommandHandlerIndexingCheckSizeTest;
import org.apache.ignite.util.GridCommandHandlerIndexingClusterByClassTest;
import org.apache.ignite.util.GridCommandHandlerIndexingClusterByClassWithSSLTest;
import org.apache.ignite.util.GridCommandHandlerIndexingTest;
import org.apache.ignite.util.GridCommandHandlerIndexingWithSSLTest;
import org.apache.ignite.util.GridCommandHandlerInterruptCommandTest;
import org.apache.ignite.util.GridCommandHandlerMetadataTest;
import org.apache.ignite.util.GridCommandHandlerPropertiesTest;
import org.apache.ignite.util.GridCommandHandlerSslTest;
import org.apache.ignite.util.GridCommandHandlerTest;
import org.apache.ignite.util.GridCommandHandlerTracingConfigurationTest;
import org.apache.ignite.util.GridCommandHandlerWithSSLTest;
import org.apache.ignite.util.KillCommandsCommandShTest;
import org.apache.ignite.util.MetricCommandTest;
import org.apache.ignite.util.PerformanceStatisticsCommandTest;
import org.apache.ignite.util.SystemViewCommandTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for control utility.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    CommandHandlerParsingTest.class,

    GridCommandHandlerTest.class,
    GridCommandHandlerWithSSLTest.class,
    GridCommandHandlerClusterByClassTest.class,
    GridCommandHandlerClusterByClassWithSSLTest.class,
    GridCommandHandlerSslTest.class,

    GridCommandHandlerSslWithSecurityTest.class,

    GridCommandHandlerBrokenIndexTest.class,
    GridCommandHandlerIndexingTest.class,
    GridCommandHandlerIndexingWithSSLTest.class,
    GridCommandHandlerIndexingClusterByClassTest.class,
    GridCommandHandlerIndexingClusterByClassWithSSLTest.class,
    GridCommandHandlerIndexingCheckSizeTest.class,
    GridCommandHandlerCheckIndexesInlineSizeTest.class,
    GridCommandHandlerInterruptCommandTest.class,
    GridCommandHandlerMetadataTest.class,

    KillCommandsCommandShTest.class,

    BaselineEventsLocalTest.class,
    BaselineEventsRemoteTest.class,

    GridCommandHandlerIndexForceRebuildTest.class,
    GridCommandHandlerIndexListTest.class,
    GridCommandHandlerIndexRebuildStatusTest.class,

    GridCommandHandlerTracingConfigurationTest.class,

    GridCommandHandlerPropertiesTest.class,

    GridCommandHandlerDefragmentationTest.class,

    SystemViewCommandTest.class,
    MetricCommandTest.class,
    PerformanceStatisticsCommandTest.class,

    IgniteIndexReaderTest.class
})
public class IgniteControlUtilityTestSuite {
}
