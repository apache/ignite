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

package org.apache.ignite.ml.selection;

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.ml.selection.cv.CrossValidationTest;
import org.apache.ignite.ml.selection.paramgrid.ParameterSetGeneratorTest;
import org.apache.ignite.ml.selection.scoring.cursor.CacheBasedLabelPairCursorTest;
import org.apache.ignite.ml.selection.scoring.cursor.LocalLabelPairCursorTest;
import org.apache.ignite.ml.selection.scoring.evaluator.EvaluatorTest;
import org.apache.ignite.ml.selection.scoring.metric.AccuracyTest;
import org.apache.ignite.ml.selection.scoring.metric.FmeasureTest;
import org.apache.ignite.ml.selection.scoring.metric.PrecisionTest;
import org.apache.ignite.ml.selection.scoring.metric.RecallTest;
import org.apache.ignite.ml.selection.split.TrainTestDatasetSplitterTest;
import org.apache.ignite.ml.selection.split.mapper.SHA256UniformMapperTest;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

/**
 * Test suite for all tests located in org.apache.ignite.ml.selection.* package.
 */
@RunWith(AllTests.class)
public class SelectionTestSuite {
    /** */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite();

        /** JUnit 4 tests. */
        suite.addTest(new JUnit4TestAdapter(CrossValidationTest.class));
        suite.addTest(new JUnit4TestAdapter(ParameterSetGeneratorTest.class));
        suite.addTest(new JUnit4TestAdapter(LocalLabelPairCursorTest.class));
        suite.addTest(new JUnit4TestAdapter(AccuracyTest.class));
        suite.addTest(new JUnit4TestAdapter(PrecisionTest.class));
        suite.addTest(new JUnit4TestAdapter(RecallTest.class));
        suite.addTest(new JUnit4TestAdapter(FmeasureTest.class));
        suite.addTest(new JUnit4TestAdapter(SHA256UniformMapperTest.class));
        suite.addTest(new JUnit4TestAdapter(TrainTestDatasetSplitterTest.class));

        /** JUnit 3 tests. */
        suite.addTestSuite(EvaluatorTest.class);
        suite.addTestSuite(CacheBasedLabelPairCursorTest.class);

        return suite;
    }
}
