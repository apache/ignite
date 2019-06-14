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

package org.apache.ignite.ml.composition;

import org.apache.ignite.ml.composition.bagging.BaggingTest;
import org.apache.ignite.ml.composition.boosting.GDBTrainerTest;
import org.apache.ignite.ml.composition.predictionsaggregator.MeanValuePredictionsAggregatorTest;
import org.apache.ignite.ml.composition.predictionsaggregator.OnMajorityPredictionsAggregatorTest;
import org.apache.ignite.ml.composition.predictionsaggregator.WeightedPredictionsAggregatorTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for all ensemble models tests.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    GDBTrainerTest.class,
    MeanValuePredictionsAggregatorTest.class,
    OnMajorityPredictionsAggregatorTest.class,
    BaggingTest.class,
    StackingTest.class,
    WeightedPredictionsAggregatorTest.class
})
public class CompositionTestSuite {
}
