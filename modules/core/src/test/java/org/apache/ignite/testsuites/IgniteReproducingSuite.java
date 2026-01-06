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

package org.apache.ignite.testsuites;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshallerSelfTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.TestTemplateInvocationContextProvider;

/**
 * Test suite for cycled run tests on PR code. <br>
 * This empty suite may be used in case it is needed to run
 * some test subset to reproduce an issue.<br>
 *
 * You may launch and check results on
 * https://ci.ignite.apache.org/viewType.html?buildTypeId=Ignite20Tests_IgniteReproducingSuite
 *
 * This suite is not included into main build.
 */
@ExtendWith(IgniteReproducingSuite.DynamicReproducingSuite.class)
public class IgniteReproducingSuite {

    public static class DynamicReproducingSuite implements TestTemplateInvocationContextProvider {

        /**
         * @return List of test(s) for reproduction some problem.
         */
        private static List<Class<?>> classes() {
            List<Class<?>> suite = new ArrayList<>();

            suite.add(TestStub.class);

            // uncomment to add some test
            for (int i = 0; i < 500; i++)
                suite.add(OptimizedMarshallerSelfTest.class);

            return suite;
        }

        @Override
        public boolean supportsTestTemplate(org.junit.jupiter.api.extension.ExtensionContext context) {
            return true;
        }

        @Override
        public java.util.stream.Stream<org.junit.jupiter.api.extension.TestTemplateInvocationContext> provideTestTemplateInvocationContexts(
                org.junit.jupiter.api.extension.ExtensionContext context) {

            return classes().stream()
                    .map(testClass -> new org.junit.jupiter.api.extension.TestTemplateInvocationContext() {
                        @Override
                        public String getDisplayName(int invocationIndex) {
                            return testClass.getSimpleName();
                        }

                        @Override
                        public List<org.junit.jupiter.api.extension.Extension> getAdditionalExtensions() {
                            return java.util.Collections.emptyList();
                        }
                    });
        }
    }

    /**
     * IMPL NOTE execution of the (empty) test suite was failing with NPE without this stub.
     */
    @Disabled
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    public static class TestStub {
        @Test
        public void dummy() {
        }
    }
}
