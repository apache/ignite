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

package org.apache.ignite.testframework.test;

import org.apache.ignite.testframework.junits.IgniteConfigVariationsAbstractTest;
import org.junit.AfterClass;
import org.junit.Test;

/**
 * Test for {@link IgniteConfigVariationsAbstractTest} subclasses execution.
 */
public class ConfigVariationsExecutionTest extends IgniteConfigVariationsAbstractTest {
    /** Boolean for checking test execution */
    private static boolean testExecuted;

    /** Executes after test class. Checks that {@link #testCheck()} method was executed indeed. */
    @AfterClass
    public static void validatetestExecution() {
        assertTrue(testExecuted);
    }

    /** JUnit test method. */
    @Test
    public void testCheck() {
        testExecuted = true;
    }
}
