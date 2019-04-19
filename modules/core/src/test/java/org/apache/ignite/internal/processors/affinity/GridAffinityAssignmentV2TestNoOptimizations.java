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

package org.apache.ignite.internal.processors.affinity;

import org.apache.ignite.IgniteSystemProperties;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link GridAffinityAssignment} without IGNITE_DISABLE_AFFINITY_MEMORY_OPTIMIZATION.
 */
@RunWith(JUnit4.class)
public class GridAffinityAssignmentV2TestNoOptimizations extends GridAffinityAssignmentV2Test {
    /** */
    @BeforeClass
    public static void beforeTests() {
        System.setProperty(IgniteSystemProperties.IGNITE_DISABLE_AFFINITY_MEMORY_OPTIMIZATION, "true");
    }

    /** */
    @AfterClass
    public static void afterTests() {
        System.clearProperty(IgniteSystemProperties.IGNITE_DISABLE_AFFINITY_MEMORY_OPTIMIZATION);
    }
}
