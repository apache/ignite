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

package org.apache.ignite.ml;

import org.apache.ignite.ml.clustering.ClusteringTestSuite;
import org.apache.ignite.ml.common.CommonTestSuite;
import org.apache.ignite.ml.composition.CompositionTestSuite;
import org.apache.ignite.ml.dataset.DatasetTestSuite;
import org.apache.ignite.ml.environment.EnvironmentTestSuite;
import org.apache.ignite.ml.genetic.GAGridTestSuite;
import org.apache.ignite.ml.knn.KNNTestSuite;
import org.apache.ignite.ml.math.MathImplMainTestSuite;
import org.apache.ignite.ml.nn.MLPTestSuite;
import org.apache.ignite.ml.preprocessing.PreprocessingTestSuite;
import org.apache.ignite.ml.regressions.RegressionsTestSuite;
import org.apache.ignite.ml.selection.SelectionTestSuite;
import org.apache.ignite.ml.structures.StructuresTestSuite;
import org.apache.ignite.ml.svm.SVMTestSuite;
import org.apache.ignite.ml.tree.DecisionTreeTestSuite;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for all module tests. IMPL NOTE tests in {@code org.apache.ignite.ml.tree.performance} are not
 * included here because these are intended only for manual execution.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    MathImplMainTestSuite.class,
    RegressionsTestSuite.class,
    SVMTestSuite.class,
    ClusteringTestSuite.class,
    DecisionTreeTestSuite.class,
    KNNTestSuite.class,
    MLPTestSuite.class,
    DatasetTestSuite.class,
    PreprocessingTestSuite.class,
    GAGridTestSuite.class,
    SelectionTestSuite.class,
    CompositionTestSuite.class,
    EnvironmentTestSuite.class,
    StructuresTestSuite.class,
    CommonTestSuite.class
})
public class IgniteMLTestSuite {
    // No-op.
}
