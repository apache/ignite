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

package org.apache.ignite.tensorflow.core;

import org.apache.ignite.tensorflow.core.longrunning.LongRunningProcessManagerTest;
import org.apache.ignite.tensorflow.core.longrunning.task.LongRunningProcessClearTaskTest;
import org.apache.ignite.tensorflow.core.longrunning.task.LongRunningProcessPingTaskTest;
import org.apache.ignite.tensorflow.core.longrunning.task.LongRunningProcessStartTaskTest;
import org.apache.ignite.tensorflow.core.longrunning.task.LongRunningProcessStopTaskTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for all tests in core package.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    ProcessManagerWrapperTest.class,
    LongRunningProcessClearTaskTest.class,
    LongRunningProcessPingTaskTest.class,
    LongRunningProcessStartTaskTest.class,
    LongRunningProcessStopTaskTest.class,
    LongRunningProcessManagerTest.class
})
public class CoreTestSuite {
    // No-op.
}
