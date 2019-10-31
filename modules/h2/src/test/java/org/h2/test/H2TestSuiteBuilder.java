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

package org.h2.test;

import java.sql.SQLException;
import junit.framework.TestResult;
import junit.framework.TestSuite;

/**
 * TestSuite generator to adapt suite to be run one-by-one with JUnit.
 */
public class H2TestSuiteBuilder extends TestAll {
    /** Test suite. */
    private TestSuite suite;

    /**
     * Constructor.
     */
    public H2TestSuiteBuilder() {
        travis = true;
        // Force test failure
        stopOnError = true;

        // Defaults, copied from base class (TestAll).
        smallLog = big = networked = memory = ssl = false;
        diskResult = traceSystemOut = diskUndo = false;
        traceTest = false;
        defrag = false;
        traceLevelFile = throttle = 0;
        cipher = null;
    }

    /** {@inheritDoc} */
    @Override protected void addTest(TestBase test) {
        suite.addTest(new H2TestCase(this, test));
    }

    /**
     * @param suiteClass Suite class.
     * @param baseTests Include base suite to suite.
     * @return Suite suite.
     */
    public TestSuite buildSuite(Class<?> suiteClass, boolean baseTests) {
        return buildSuite(suiteClass, baseTests, false);
    }

    /**
     * @param suiteClass Suite class.
     * @param baseTests Include base suite to suite.
     * @param additionalTests Include additional suite to suite.
     * @return Suite suite.
     */
    public TestSuite buildSuite(Class<?> suiteClass, boolean baseTests, boolean additionalTests) {
        suite = new TestSuite(suiteClass.getName()) {
            /** {@inheritDoc} */
            @Override public void run(TestResult result) {
                try {
                    beforeTest();
                    super.run(result);
                }
                finally {
                    afterTest();
                }
            }
        };

        try {
            if (baseTests)
                test();

            if (additionalTests)
                testUnit();
        }
        catch (SQLException e) {
            assert false : e;
        }

        TestSuite suite0 = suite;

        suite = null;

        return suite0;
    }

    /** {@inheritDoc} */
    @Override public void beforeTest() {
        try {
            super.beforeTest();
        }
        catch (Exception e) {
            e.printStackTrace(System.err);
            throw new AssertionError("Failed to start suite.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void afterTest() {
        super.afterTest();
    }
}
