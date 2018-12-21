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

package org.apache.ignite.ml.tree;

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.ml.tree.data.DecisionTreeDataTest;
import org.apache.ignite.ml.tree.impurity.gini.GiniImpurityMeasureCalculatorTest;
import org.apache.ignite.ml.tree.impurity.gini.GiniImpurityMeasureTest;
import org.apache.ignite.ml.tree.impurity.mse.MSEImpurityMeasureCalculatorTest;
import org.apache.ignite.ml.tree.impurity.mse.MSEImpurityMeasureTest;
import org.apache.ignite.ml.tree.impurity.util.SimpleStepFunctionCompressorTest;
import org.apache.ignite.ml.tree.impurity.util.StepFunctionTest;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Test suite for all tests located in {@link org.apache.ignite.ml.tree} package.
 */
@RunWith(AllTests.class)
public class DecisionTreeTestSuite {
    /** */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite();

        /** JUnit 4 tests. */
        suite.addTest(new JUnit4TestAdapter(DecisionTreeClassificationTrainerTest.class));
        suite.addTest(new JUnit4TestAdapter(DecisionTreeRegressionTrainerTest.class));
        suite.addTest(new JUnit4TestAdapter(DecisionTreeDataTest.class));
        suite.addTest(new JUnit4TestAdapter(GiniImpurityMeasureCalculatorTest.class));
        suite.addTest(new JUnit4TestAdapter(GiniImpurityMeasureTest.class));
        suite.addTest(new JUnit4TestAdapter(MSEImpurityMeasureCalculatorTest.class));
        suite.addTest(new JUnit4TestAdapter(MSEImpurityMeasureTest.class));
        suite.addTest(new JUnit4TestAdapter(StepFunctionTest.class));
        suite.addTest(new JUnit4TestAdapter(SimpleStepFunctionCompressorTest.class));

        /** JUnit 3 tests. */
        suite.addTestSuite(DecisionTreeRegressionTrainerIntegrationTest.class);
        suite.addTestSuite(DecisionTreeClassificationTrainerIntegrationTest.class);

        return suite;
    }
}
