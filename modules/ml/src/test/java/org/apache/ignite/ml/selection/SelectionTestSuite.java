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
        TestSuite suite = new TestSuite(SelectionTestSuite.class.getSimpleName());

        suite.addTest(new JUnit4TestAdapter(CrossValidationTest.class));
        suite.addTest(new JUnit4TestAdapter(ParameterSetGeneratorTest.class));
        suite.addTest(new JUnit4TestAdapter(LocalLabelPairCursorTest.class));
        suite.addTest(new JUnit4TestAdapter(AccuracyTest.class));
        suite.addTest(new JUnit4TestAdapter(PrecisionTest.class));
        suite.addTest(new JUnit4TestAdapter(RecallTest.class));
        suite.addTest(new JUnit4TestAdapter(FmeasureTest.class));
        suite.addTest(new JUnit4TestAdapter(SHA256UniformMapperTest.class));
        suite.addTest(new JUnit4TestAdapter(TrainTestDatasetSplitterTest.class));
        suite.addTest(new JUnit4TestAdapter(EvaluatorTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheBasedLabelPairCursorTest.class));

        return suite;
    }
}
