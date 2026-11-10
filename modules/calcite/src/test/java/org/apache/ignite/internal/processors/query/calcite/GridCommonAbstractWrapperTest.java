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

package org.apache.ignite.internal.processors.query.calcite;

import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;

import static org.apache.ignite.tools.junit.JUnitTeamcityReporter.escapeForTeamcity;

/** */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class GridCommonAbstractWrapperTest extends GridCommonAbstractTest {
    /** */
    @BeforeEach
    void beforeTest(TestInfo testInfo) {
        printTestName(testInfo, true);
    }

    /** */
    @AfterEach
    void afterTest(TestInfo testInfo) {
        printTestName(testInfo, false);
    }

    /** Get and print the display name of the upcoming test. */
    private void printTestName(TestInfo testInfo, boolean start) {
        // Get and print the display name of the upcoming test
        String testName = testInfo.getDisplayName();
        String testCls = testInfo.getTestClass().orElse(Object.class).getSimpleName();

        String testFullName = escapeForTeamcity(testCls + "#" + testName);

        if (start)
            U.quietAndInfo(log(), ">>> Starting test: " + testFullName + " <<<");
        else
            U.quietAndInfo(log(), ">>> Stopping test: " + testFullName + " <<<");
    }

    /** */
    @BeforeAll
    void init() {
        beforeFirstTest0();
    }

    /** {@inheritDoc} */
    @AfterAll
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }
}
