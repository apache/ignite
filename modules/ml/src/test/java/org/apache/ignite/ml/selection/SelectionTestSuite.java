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

import org.apache.ignite.ml.selection.cv.CrossValidationScoreCalculatorTest;
import org.apache.ignite.ml.selection.score.AccuracyScoreCalculatorTest;
import org.apache.ignite.ml.selection.score.util.CacheBasedTruthWithPredictionCursorTest;
import org.apache.ignite.ml.selection.score.util.LocalTruthWithPredictionCursorTest;
import org.apache.ignite.ml.selection.split.TrainTestDatasetSplitterTest;
import org.apache.ignite.ml.selection.split.mapper.SHA256UniformMapperTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for all tests located in org.apache.ignite.ml.selection.* package.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    CrossValidationScoreCalculatorTest.class,
    CacheBasedTruthWithPredictionCursorTest.class,
    LocalTruthWithPredictionCursorTest.class,
    AccuracyScoreCalculatorTest.class,
    SHA256UniformMapperTest.class,
    TrainTestDatasetSplitterTest.class
})
public class SelectionTestSuite {
    // No-op.
}
