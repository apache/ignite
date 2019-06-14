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

package org.apache.ignite.ml.clustering.gmm;

import java.util.Collections;
import java.util.List;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.environment.deploy.DeployableObject;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.stat.DistributionMixture;
import org.apache.ignite.ml.math.stat.MultivariateGaussianDistribution;

/**
 * Gaussian Mixture Model. This algorithm represents a soft clustering model where each cluster is gaussian distribution
 * with own mean value and covariation matrix. Such model can predict cluster using maximum likelihood priciple (see
 * {@link #predict(Vector)}). Also * this model can estimate probability of given vector (see {@link #prob(Vector)}) and
 * compute likelihood vector where each component of it is a probability of cluster of mixture (see {@link
 * #likelihood(Vector)}).
 */
public class GmmModel extends DistributionMixture<MultivariateGaussianDistribution> implements IgniteModel<Vector, Double>,
    DeployableObject {
    /** Serial version uid. */
    private static final long serialVersionUID = -4484174539118240037L;

    /**
     * Creates an instance of GmmModel.
     *
     * @param componentProbs Probabilities of components.
     * @param distributions Gaussian distributions for each component.
     */
    public GmmModel(Vector componentProbs, List<MultivariateGaussianDistribution> distributions) {
        super(componentProbs, distributions);
    }

    /** {@inheritDoc} */
    @Override public Double predict(Vector input) {
        return (double)likelihood(input).maxElement().index();
    }

    /** {@inheritDoc} */
    @Override public List<Object> getDependencies() {
        return Collections.emptyList();
    }
}
