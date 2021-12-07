/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.testframework;

import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;
import static org.junit.platform.testkit.engine.EventConditions.finishedSuccessfully;
import static org.junit.platform.testkit.engine.EventConditions.finishedWithFailure;
import static org.junit.platform.testkit.engine.EventConditions.type;

import org.assertj.core.api.Condition;
import org.junit.platform.testkit.engine.EngineExecutionResults;
import org.junit.platform.testkit.engine.EngineTestKit;
import org.junit.platform.testkit.engine.EventType;

/**
 * Utilities for writing tests for JUnit extensions.
 *
 * @see <a href="https://junit.org/junit5/docs/current/user-guide/#testkit">JUnit Platform Test Kit</a>
 */
class JunitExtensionTestUtils {
    /**
     * Executes the given test class on the Jupiter test engine.
     */
    private static EngineExecutionResults execute(Class<?> testClass) {
        return EngineTestKit.engine("junit-jupiter")
                .selectors(selectClass(testClass))
                .execute();
    }

    /**
     * Executes the given test class and checks that it has run all its tests successfully.
     */
    static void assertExecutesSuccessfully(Class<?> testClass) {
        execute(testClass)
                .allEvents()
                .assertThatEvents()
                .filteredOn(type(EventType.FINISHED))
                .isNotEmpty()
                .are(finishedSuccessfully());
    }

    /**
     * Executes the given test class and checks that it fails with matching error conditions.
     */
    @SafeVarargs
    static void assertExecutesWithFailure(Class<?> testClass, Condition<Throwable>... conditions) {
        execute(testClass)
                .allEvents()
                .assertThatEvents()
                .filteredOn(finishedWithFailure())
                .isNotEmpty()
                .are(finishedWithFailure(conditions));
    }
}
