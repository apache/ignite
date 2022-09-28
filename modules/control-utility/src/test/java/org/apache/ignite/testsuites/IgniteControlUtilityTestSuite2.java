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

import org.apache.ignite.internal.commandline.indexreader.IgniteIndexReaderTest;
import org.apache.ignite.util.CacheMetricsCommandTest;
import org.apache.ignite.util.GridCommandHandlerConsistencyBinaryTest;
import org.apache.ignite.util.GridCommandHandlerConsistencyCountersTest;
import org.apache.ignite.util.GridCommandHandlerConsistencyRepairCorrectnessAtomicTest;
import org.apache.ignite.util.GridCommandHandlerConsistencySensitiveTest;
import org.apache.ignite.util.GridCommandHandlerConsistencyTest;
import org.apache.ignite.util.GridCommandHandlerDefragmentationTest;
import org.apache.ignite.util.GridCommandHandlerIndexForceRebuildTest;
import org.apache.ignite.util.GridCommandHandlerIndexListTest;
import org.apache.ignite.util.GridCommandHandlerIndexRebuildStatusTest;
import org.apache.ignite.util.GridCommandHandlerPropertiesTest;
import org.apache.ignite.util.GridCommandHandlerScheduleIndexRebuildTest;
import org.apache.ignite.util.GridCommandHandlerTracingConfigurationTest;
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
    GridCommandHandlerIndexForceRebuildTest.class,
    GridCommandHandlerIndexListTest.class,
    GridCommandHandlerIndexRebuildStatusTest.class,
    GridCommandHandlerScheduleIndexRebuildTest.class,

    GridCommandHandlerTracingConfigurationTest.class,

    GridCommandHandlerPropertiesTest.class,

    GridCommandHandlerDefragmentationTest.class,

    GridCommandHandlerConsistencyTest.class,
    GridCommandHandlerConsistencyCountersTest.class,
    GridCommandHandlerConsistencyBinaryTest.class,
    GridCommandHandlerConsistencySensitiveTest.class,
    GridCommandHandlerConsistencyRepairCorrectnessAtomicTest.class,

    SystemViewCommandTest.class,
    MetricCommandTest.class,
    PerformanceStatisticsCommandTest.class,
    CacheMetricsCommandTest.class,

    IgniteIndexReaderTest.class
})
public class IgniteControlUtilityTestSuite2 {
}
