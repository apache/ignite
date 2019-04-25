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

package org.apache.ignite.ml.selection;

import org.apache.ignite.ml.selection.cv.CrossValidationTest;
import org.apache.ignite.ml.selection.paramgrid.ParameterSetGeneratorTest;
import org.apache.ignite.ml.selection.scoring.cursor.CacheBasedLabelPairCursorTest;
import org.apache.ignite.ml.selection.scoring.cursor.LocalLabelPairCursorTest;
import org.apache.ignite.ml.selection.scoring.evaluator.BinaryClassificationEvaluatorTest;
import org.apache.ignite.ml.selection.scoring.metric.classification.*;
import org.apache.ignite.ml.selection.scoring.metric.regression.RegressionMetricsTest;
import org.apache.ignite.ml.selection.split.TrainTestDatasetSplitterTest;
import org.apache.ignite.ml.selection.split.mapper.SHA256UniformMapperTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for all tests located in org.apache.ignite.ml.selection.* package.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    CrossValidationTest.class,
    ParameterSetGeneratorTest.class,
    LocalLabelPairCursorTest.class,
    AccuracyTest.class,
    PrecisionTest.class,
    RecallTest.class,
    FmeasureTest.class,
    SHA256UniformMapperTest.class,
    TrainTestDatasetSplitterTest.class,
    CacheBasedLabelPairCursorTest.class,
    BinaryClassificationMetricsTest.class,
    BinaryClassificationMetricsValuesTest.class,
    BinaryClassificationEvaluatorTest.class,
    RegressionMetricsTest.class
})
public class SelectionTestSuite {
}
