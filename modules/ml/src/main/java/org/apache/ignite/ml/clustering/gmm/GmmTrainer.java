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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.math.stat.MultivariateGaussianDistribution;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.trainers.FeatureLabelExtractor;

public class GmmTrainer extends DatasetTrainer<GmmModel, Double> {
    private double eps = 1e-3;
    private int countOfComponents = 2;
    private int maxCountOfIterations = 10;
    private long seed = System.currentTimeMillis();
    private List<Vector> initialMeans;

    public GmmTrainer() {
    }

    public GmmTrainer(int countOfComponents, int maxCountOfIterations) {
        this.countOfComponents = countOfComponents;
        this.maxCountOfIterations = maxCountOfIterations;
    }

    @Override
    public <K, V> GmmModel fit(DatasetBuilder<K, V> datasetBuilder, FeatureLabelExtractor<K, V, Double> extractor) {
        try (Dataset<EmptyContext, GmmPartitionData> dataset = datasetBuilder.build(
            LearningEnvironmentBuilder.defaultBuilder(),
            new EmptyContextBuilder<>(),
            new GmmPartitionData.Builder<>(extractor, countOfComponents)
        )) {
            return fit(dataset);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public GmmTrainer withSeed(long seed) {
        this.seed = seed;
        return this;
    }

    public GmmTrainer withCountOfComponents(int numberOfComponents) {
        A.ensure(numberOfComponents > 0, "Number of components in GMM cannot equal 0");

        this.countOfComponents = numberOfComponents;
        initialMeans = null;
        return this;
    }

    public GmmTrainer withInitialMeans(List<Vector> means) {
        A.notEmpty(means, "GMM should starts with non empty initial components list");

        this.initialMeans = means;
        this.countOfComponents = means.size();
        return this;
    }

    public GmmTrainer withMaxCountIterations(int maxCountOfIterations) {
        A.ensure(maxCountOfIterations > 0, "Max count iterations cannot be less or equal zero");

        this.maxCountOfIterations = maxCountOfIterations;
        return this;
    }

    public GmmTrainer withEps(double eps) {
        A.ensure(eps > 0 && eps < 1.0, "Max divergence beween iterations should be between 0.0 and 1.0");

        this.eps = eps;
        return this;
    }

    private GmmModel fit(Dataset<EmptyContext, GmmPartitionData> dataset) {
        GmmModel model = init(dataset);
        boolean isConverged = false;
        int countOfIterations = 0;
        while (!isConverged) {
            MeanWithClusterProbAggregator.AggregatedStats stats = MeanWithClusterProbAggregator.aggreateStats(dataset);
            Vector clusterProbs = stats.clusterProbabilities();
            List<Vector> newMeans = stats.means();

            A.ensure(newMeans.size() == model.countOfComponents(), "newMeans.size() == count of components");
            A.ensure(newMeans.get(0).size() == initialMeans.get(0).size(), "newMeans[0].size() == initialMeans[0].size()");
            List<Matrix> newCovs = CovarianceMatricesAggregator.computeCovariances(dataset, clusterProbs, newMeans);

            List<MultivariateGaussianDistribution> components = buildComponents(newMeans, newCovs);
            GmmModel newModel = new GmmModel(clusterProbs, components);

            countOfIterations += 1;
            isConverged = isConverged(model, newModel) || countOfIterations > maxCountOfIterations;
            model = newModel;

            if (!isConverged)
                dataset.compute(data -> GmmPartitionData.updatePcxi(data, clusterProbs, components));
        }

        return model;
    }

    private GmmModel init(Dataset<EmptyContext, GmmPartitionData> dataset) {
        if (initialMeans == null) {
            initialMeans = dataset.compute(
                selectNRandomXsMapper(countOfComponents),
                selectNRandomXsReducer(countOfComponents, seed)
            );
        }

        dataset.compute(data -> GmmPartitionData.estimateLikelihoodClusters(data, initialMeans));

        List<Matrix> initialCovs = CovarianceMatricesAggregator.computeCovariances(
            dataset,
            VectorUtils.of(DoubleStream.generate(() -> 1. / countOfComponents).limit(countOfComponents).toArray()),
            initialMeans);

        List<MultivariateGaussianDistribution> distributions = new ArrayList<>();
        for (int i = 0; i < countOfComponents; i++)
            distributions.add(new MultivariateGaussianDistribution(initialMeans.get(i), initialCovs.get(i)));

        return new GmmModel(
            VectorUtils.of(DoubleStream.generate(() -> 1. / countOfComponents).limit(countOfComponents).toArray()),
            distributions
        );
    }

    private List<MultivariateGaussianDistribution> buildComponents(List<Vector> means, List<Matrix> covs) {
        A.ensure(means.size() == covs.size(), "means.size() == covs.size()");

        List<MultivariateGaussianDistribution> res = new ArrayList<>();
        for (int i = 0; i < means.size(); i++)
            res.add(new MultivariateGaussianDistribution(means.get(i), covs.get(i)));

        return res;
    }

    private boolean isConverged(GmmModel oldModel, GmmModel newModel) {
        A.ensure(oldModel.countOfComponents() == newModel.countOfComponents(),
            "oldModel.countOfComponents() == newModel.countOfComponents()");

        boolean isConverged = true;
        for (int i = 0; i < oldModel.countOfComponents(); i++) {
            MultivariateGaussianDistribution d1 = oldModel.distributions().get(i);
            MultivariateGaussianDistribution d2 = newModel.distributions().get(i);

            isConverged = isConverged && Math.sqrt(d1.mean().getDistanceSquared(d2.mean())) < eps;
        }

        return isConverged;
    }

    @Override public boolean isUpdateable(GmmModel mdl) {
        return false;
    }

    @Override protected <K, V> GmmModel updateModel(GmmModel mdl, DatasetBuilder<K, V> datasetBuilder,
        FeatureLabelExtractor<K, V, Double> extractor) {

        return null;
    }

    private static IgniteBiFunction<GmmPartitionData, LearningEnvironment, List<Vector>> selectNRandomXsMapper(int n) {
        return (data, env) -> env.randomNumbersGenerator().ints(n, 0, data.size())
            .mapToObj(data::getX)
            .collect(Collectors.toList());
    }

    private static IgniteBinaryOperator<List<Vector>> selectNRandomXsReducer(int n, long seed) {
        return (l, r) -> {
            A.ensure(l != null || r != null, "l != null || r != null");
            if (l == null)
                return checkList(n, r);
            if (r == null)
                return checkList(n, l);

            List<Vector> res = new ArrayList<>();
            res.addAll(l);
            res.addAll(r);
            Collections.shuffle(res, new Random(seed));

            return res.stream().limit(n).collect(Collectors.toList());
        };
    }

    private static List<Vector> checkList(int expectedSize, List<Vector> xs) {
        A.ensure(xs.size() == expectedSize, "xs.size() == expectedSize");
        return xs;
    }
}
