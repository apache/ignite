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

import org.apache.ignite.internal.GridContinuousJobAnnotationSelfTest;
import org.apache.ignite.internal.GridContinuousJobSiblingsSelfTest;
import org.apache.ignite.internal.GridContinuousTaskSelfTest;
import org.apache.ignite.internal.GridTaskContinuousMapperSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Continuous task self-test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    GridContinuousJobAnnotationSelfTest.class,
    GridContinuousJobSiblingsSelfTest.class,
    GridContinuousTaskSelfTest.class,
    GridTaskContinuousMapperSelfTest.class
})
public class IgniteContinuousTaskSelfTestSuite {
}
