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

import junit.framework.TestResult;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.runners.model.Statement;

/**
 * Supports compatibility with old tests that expect specific threading behavior of JUnit 3 TestCase class,
 * inherited deprecated assertions and specific old interface for GridTestUtils.
 */
@SuppressWarnings({"TransientFieldInNonSerializableClass"})
public abstract class LegacySupport extends LegacyConfigVariationsSupport {
    /**
     * Supports obtaining test name for JUnit4 framework in a way that makes it available for legacy methods invoked
     * from {@code runTest(Statement)}.
     */
    @Rule public transient TestName nameRule = new TestName();

    /**
     * Gets the name of the currently executed test case.
     *
     * @return Name of the currently executed test case.
     */
    @Override public String getName() {
        return nameRule.getMethodName();
    }

    /** Runs test code in between {@code setUp} and {@code tearDown}. */
    abstract void runTest(Statement testRoutine) throws Throwable;

    /**
     * Runs the bare test sequence like in JUnit 3 class TestCase.
     *
     * @throws Throwable if any exception is thrown
     */
    final void runTestCase(Statement testRoutine) throws Throwable {
        Throwable e = null;
        setUp();
        try {
            runTest(testRoutine);
        } catch (Throwable running) {
            e = running;
        } finally {
            try {
                tearDown();
            } catch (Throwable tearingDown) {
                if (e == null) e = tearingDown;
            }
        }
        if (e != null) throw e;
    }

    /**
     * {@inheritDoc}
     * <p>
     * This method is here only for subclass to pretend implementing particular interface expected by some utility
     * methods in GridTestUtils.</p>
     *
     * @return Nothing.
     */
    @Override public int countTestCases() {
        throw new UnsupportedOperationException("This method is not expected to be invoked: countTestCases() at test: "
            + getName());
    }

    /**
     * {@inheritDoc}
     * <p>
     * This method is here only for subclass to pretend implementing particular interface expected by some utility
     * methods in GridTestUtils.</p>
     */
    @Override public void run(TestResult res) {
        throw new UnsupportedOperationException("This method is not intended to be invoked: run(TestResult) at test: "
            + getName());
    }
}
