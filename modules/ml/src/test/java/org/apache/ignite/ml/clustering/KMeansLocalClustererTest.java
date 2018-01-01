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

package org.apache.ignite.ml.clustering;

import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.junit.Assert;
import org.junit.Test;

/** */
public class KMeansLocalClustererTest {
    /**
     * Two points, one cluster, one iteration
     */
    @Test
    public void testPerformClusterAnalysisDegenerate() {
        KMeansLocalClusterer clusterer = new KMeansLocalClusterer(new EuclideanDistance(), 1, 1L);

        double[] v1 = new double[] {1959, 325100};
        double[] v2 = new double[] {1960, 373200};

        DenseLocalOnHeapMatrix points = new DenseLocalOnHeapMatrix(new double[][] {
            v1,
            v2});

        KMeansModel mdl = clusterer.cluster(points, 1);

        Assert.assertEquals(1, mdl.centers().length);
        Assert.assertEquals(2, mdl.centers()[0].size());
    }
}
