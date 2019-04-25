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

import org.apache.ignite.spi.eventstorage.memory.GridMemoryEventStorageMultiThreadedSelfTest;
import org.apache.ignite.spi.eventstorage.memory.GridMemoryEventStorageSpiConfigSelfTest;
import org.apache.ignite.spi.eventstorage.memory.GridMemoryEventStorageSpiSelfTest;
import org.apache.ignite.spi.eventstorage.memory.GridMemoryEventStorageSpiStartStopSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Event storage test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    GridMemoryEventStorageSpiSelfTest.class,
    GridMemoryEventStorageSpiStartStopSelfTest.class,
    GridMemoryEventStorageMultiThreadedSelfTest.class,
    GridMemoryEventStorageSpiConfigSelfTest.class
})
public class IgniteSpiEventStorageSelfTestSuite {
}
