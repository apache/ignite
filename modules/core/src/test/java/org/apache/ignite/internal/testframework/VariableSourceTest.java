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

import static org.apache.ignite.internal.testframework.JunitExtensionTestUtils.assertExecutesSuccessfully;
import static org.apache.ignite.internal.testframework.JunitExtensionTestUtils.assertExecutesWithFailure;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.platform.testkit.engine.TestExecutionResultConditions.instanceOf;
import static org.junit.platform.testkit.engine.TestExecutionResultConditions.message;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

class VariableSourceTest {
    /**
     * Test class for the {@link #testStaticFieldInjection()} test.
     */
    static class NormalVariableSourceTest {
        private static final List<Object[]> arguments = List.of(
                new Object[] {0, "foo"},
                new Object[] {1, "bar"},
                new Object[] {2, "baz"}
        );

        @ParameterizedTest
        @VariableSource("arguments")
        public void test(int i, String argument) {
            assertEquals(arguments.get(i)[1], argument);
        }
    }

    /**
     * Tests the happy case of the {@link VariableSource} usage.
     */
    @Test
    void testStaticFieldInjection() {
        assertExecutesSuccessfully(NormalVariableSourceTest.class);
    }

    /**
     * Test class for the {@link #testNonStaticFieldInjection()} test.
     */
    static class NonStaticVariableSourceTest {
        private final String arguments = "foo";

        @ParameterizedTest
        @VariableSource("arguments")
        public void test(String argument) {
            fail("Should not reach here");
        }
    }

    /**
     * Tests the case when the {@code VariableSource} variable is not static.
     */
    @Test
    void testNonStaticFieldInjection() {
        assertExecutesWithFailure(
                NonStaticVariableSourceTest.class,
                instanceOf(IllegalArgumentException.class),
                message(m -> m.contains("must be static"))
        );
    }

    /**
     * Test class for the {@link #testMissingVariableSource()} test.
     */
    static class MissingVariableSourceTest {
        @ParameterizedTest
        @VariableSource("missing")
        public void test(String argument) {
            fail("Should not reach here");
        }
    }

    /**
     * Tests the case when the {@code VariableSource} variable is not declared.
     */
    @Test
    void testMissingVariableSource() {
        assertExecutesWithFailure(
                MissingVariableSourceTest.class,
                instanceOf(NoSuchFieldException.class),
                message(m -> m.contains("missing"))
        );
    }
}
