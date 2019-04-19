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

package org.apache.ignite.ml.regressions;

import org.apache.ignite.ml.regressions.linear.LinearRegressionLSQRTrainerTest;
import org.apache.ignite.ml.regressions.linear.LinearRegressionModelTest;
import org.apache.ignite.ml.regressions.linear.LinearRegressionSGDTrainerTest;
import org.apache.ignite.ml.regressions.logistic.LogisticRegressionModelTest;
import org.apache.ignite.ml.regressions.logistic.LogisticRegressionSGDTrainerTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for all tests located in org.apache.ignite.ml.regressions.* package.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    LinearRegressionModelTest.class,
    LinearRegressionLSQRTrainerTest.class,
    LinearRegressionSGDTrainerTest.class,
    LogisticRegressionModelTest.class,
    LogisticRegressionSGDTrainerTest.class
})
public class RegressionsTestSuite {
    // No-op.
}
