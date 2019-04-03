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

package org.apache.ignite.testframework.junits;

import org.junit.After;
import org.junit.Before;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;

/**
 * Provides the basic functionality for Ignite test conditions and environment.
 */
@SuppressWarnings({"TransientFieldInNonSerializableClass", "ExtendsUtilityClass"})
public class TestConditionsAware extends JUnitAssertAware {
    /**
     * Supports obtaining test name for JUnit4 framework in a way that makes it available for methods invoked
     * from {@code runTest(Statement)}.
     */
    @Rule public transient TestName nameRule = new TestName();

    /**
     * Gets the name of the currently executed test case.
     *
     * @return Name of the currently executed test case.
     */
    public String getName() {
        return nameRule.getMethodName();
    }

    /**
     * Called before execution of every test method in class.
     * <p>
     * Do not annotate with {@link Before} in overriding methods.</p>
     *
     * @throws Exception If failed. {@link #afterTest()} will be called anyway.
     */
    protected void beforeTest() throws Exception {
        // No-op.
    }

    /**
     * Called after execution of every test method in class or if {@link #beforeTest()} failed without test method
     * execution.
     * <p>
     * Do not annotate with {@link After} in overriding methods.</p>
     *
     * @throws Exception If failed.
     */
    protected void afterTest() throws Exception {
        // No-op.
    }

    /**
     * Called before execution of all test methods in class.
     * <p>
     * Do not annotate with {@link BeforeClass} in overriding methods.</p>
     *
     * @throws Exception If failed. {@link #afterTestsStopped()} will be called in this case.
     */
    protected void beforeTestsStarted() throws Exception {
        // No-op.
    }

    /**
     * Called after execution of all test methods in class or if {@link #beforeTestsStarted()} failed without
     * execution of any test methods.
     * <p>
     * Do not annotate with {@link AfterClass} in overriding methods.</p>
     *
     * @throws Exception If failed.
     */
    protected void afterTestsStopped() throws Exception {
        // No-op.
    }
}
