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

package org.apache.ignite.ml.clustering;

import org.apache.ignite.ml.clustering.gmm.CovarianceMatricesAggregatorTest;
import org.apache.ignite.ml.clustering.gmm.GmmModelTest;
import org.apache.ignite.ml.clustering.gmm.GmmPartitionDataTest;
import org.apache.ignite.ml.clustering.gmm.GmmTrainerIntegrationTest;
import org.apache.ignite.ml.clustering.gmm.GmmTrainerTest;
import org.apache.ignite.ml.clustering.gmm.MeanWithClusterProbAggregatorTest;
import org.apache.ignite.ml.clustering.gmm.NewComponentStatisticsAggregatorTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for all tests located in org.apache.ignite.ml.clustering package.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    //k-means tests
    KMeansTrainerTest.class,
    KMeansModelTest.class,

    //GMM tests
    CovarianceMatricesAggregatorTest.class,
    GmmModelTest.class,
    GmmPartitionDataTest.class,
    MeanWithClusterProbAggregatorTest.class,
    GmmTrainerTest.class,
    GmmTrainerIntegrationTest.class,
    NewComponentStatisticsAggregatorTest.class
})
public class ClusteringTestSuite {
}
