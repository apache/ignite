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
        TestSuite suite = new TestSuite(DecisionTreeTestSuite.class.getSimpleName());

        suite.addTest(new JUnit4TestAdapter(DecisionTreeClassificationTrainerTest.class));
        suite.addTest(new JUnit4TestAdapter(DecisionTreeRegressionTrainerTest.class));
        suite.addTest(new JUnit4TestAdapter(DecisionTreeDataTest.class));
        suite.addTest(new JUnit4TestAdapter(GiniImpurityMeasureCalculatorTest.class));
        suite.addTest(new JUnit4TestAdapter(GiniImpurityMeasureTest.class));
        suite.addTest(new JUnit4TestAdapter(MSEImpurityMeasureCalculatorTest.class));
        suite.addTest(new JUnit4TestAdapter(MSEImpurityMeasureTest.class));
        suite.addTest(new JUnit4TestAdapter(StepFunctionTest.class));
        suite.addTest(new JUnit4TestAdapter(SimpleStepFunctionCompressorTest.class));
        suite.addTest(new JUnit4TestAdapter(DecisionTreeRegressionTrainerIntegrationTest.class));
        suite.addTest(new JUnit4TestAdapter(DecisionTreeClassificationTrainerIntegrationTest.class));

        return suite;
    }
}
