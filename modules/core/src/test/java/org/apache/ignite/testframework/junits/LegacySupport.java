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

import junit.framework.Assert; // IMPL NOTE some old tests expect inherited deprecated assertions.
import junit.framework.Test;
import junit.framework.TestResult;
import org.junit.runners.model.Statement;

/**
 * Supports compatibility with old tests that expect specific threading behavior of JUnit 3 TestCase class,
 * inherited deprecated assertions and specific old interface for GridTestUtils.
 */
@SuppressWarnings({"ExtendsUtilityClass", "deprecation"})
abstract class LegacySupport extends Assert implements Test {
    /** This method is called before a test is executed. */
    abstract void setUp() throws Exception;

    /** Runs test code in between {@code setUp} and {@code tearDown}. */
    abstract void runTest(Statement testRoutine) throws Throwable;

    /** This method is called after a test is executed. */
    abstract void tearDown() throws Exception;

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
    @Override public final int countTestCases() {
        throw new UnsupportedOperationException("this method is not expected to be invoked");
    }

    /**
     * {@inheritDoc}
     * <p>
     * This method is here only for subclass to pretend implementing particular interface expected by some utility
     * methods in GridTestUtils.</p>
     */
    @Override public final void run(TestResult res) {
        throw new UnsupportedOperationException("this method is not expected to be invoked");
    }
}
