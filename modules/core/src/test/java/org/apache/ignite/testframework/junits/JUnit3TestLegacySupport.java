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

package org.apache.ignite.testframework.junits;

import org.junit.Rule;
import org.junit.rules.TestName;

/**
 * Supports compatibility with old tests that expect specific threading behavior of JUnit 3 TestCase class,
 * inherited assertions and specific old interface for GridTestUtils.
 * @deprecated and should be removed once all tests will be refactored to use proper API.
 */
@SuppressWarnings({"TransientFieldInNonSerializableClass", "ExtendsUtilityClass"})
@Deprecated
public class JUnit3TestLegacySupport extends JUnitAssertAware {
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
