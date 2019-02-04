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

package org.apache.ignite.ml.clustering.gmm;

import java.util.List;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.stat.DistributionMixture;
import org.apache.ignite.ml.math.stat.MultivariateGaussianDistribution;

public class GmmModel extends DistributionMixture implements IgniteModel<Vector, Double> {
    public GmmModel(Vector componentProbs, List<MultivariateGaussianDistribution> distributions) {
        super(componentProbs, distributions);
    }

    @Override public Double predict(Vector input) {
        Vector likelihood = likelihood(input);
        double maxLikelihood = -1.;
        int idxOfMax = -1;

        for (int i = 0; i < likelihood.size(); i++) {
            double prob = likelihood.get(i);
            if (prob > maxLikelihood) {
                maxLikelihood = prob;
                idxOfMax = i;
            }
        }

        return (double)idxOfMax;
    }
}
