/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.testframework.junits;

import org.junit.Rule;
import org.junit.rules.TestName;
import org.junit.runners.model.Statement;

/**
 * Supports compatibility with old tests that expect specific threading behavior of JUnit 3 TestCase class,
 * inherited assertions and specific old interface for GridTestUtils.
 * @deprecated and should be removed once all tests will be refactored to use proper API.
 */
@SuppressWarnings({"TransientFieldInNonSerializableClass", "ExtendsUtilityClass"})
@Deprecated
public abstract class JUnit3TestLegacySupport extends JUnitAssertAware {
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
    public String getName() {
        return nameRule.getMethodName();
    }

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
    protected final void runTestCase(Statement testRoutine) throws Throwable {
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
     * Called before execution of every test method in class.
     * <p>
     * Do not annotate with Before in overriding methods.</p>
     *
     * @throws Exception If failed. {@link #afterTest()} will be called in this case.
     */
    protected void beforeTest() throws Exception {
        // No-op.
    }

    /**
     * Called after execution of every test method in class or if {@link #beforeTest()} failed without test method
     * execution.
     * <p>
     * Do not annotate with After in overriding methods.</p>
     *
     * @throws Exception If failed.
     */
    protected void afterTest() throws Exception {
        // No-op.
    }

    /**
     * Called before execution of all test methods in class.
     * <p>
     * Do not annotate with BeforeClass in overriding methods.</p>
     *
     * @throws Exception If failed. {@link #afterTestsStopped()} will be called in this case.
     */
    protected void beforeTestsStarted() throws Exception {
        // No-op.
    }

    /**
     * Called after execution of all test methods in class or
     * if {@link #beforeTestsStarted()} failed without execution of any test methods.
     * <p>
     * Do not annotate with AfterClass in overriding methods.</p>
     *
     * @throws Exception If failed.
     */
    protected void afterTestsStopped() throws Exception {
        // No-op.
    }
}
