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

import java.io.Serializable;
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
import org.apache.ignite.ml.math.util.MatrixUtil;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.trainers.FeatureLabelExtractor;

public class GmmTrainer extends DatasetTrainer<GmmModel, Double> implements Serializable {
    private final double EPS = 1e-4;
    private final int countOfComponents;
    private final int maxCountOfIterations;
    private final long seed;

    public GmmTrainer(int countOfComponents, int maxCountOfIterations) {
        this(countOfComponents, maxCountOfIterations, System.currentTimeMillis());
    }

    public GmmTrainer(int countOfComponents, int maxCountOfIterations, long seed) {
        this.countOfComponents = countOfComponents;
        this.maxCountOfIterations = maxCountOfIterations;
        this.seed = seed;
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

    private GmmModel fit(Dataset<EmptyContext, GmmPartitionData> dataset) {
        GmmModel model = init(dataset);
        boolean isConverged = false;
        int countOfIterations = 0;
        while (!isConverged) {
            List<MeanWithClusterProbAggregator> newMeansAndClusterProbs = updateMeans(dataset);

            int N = newMeansAndClusterProbs.get(0).getN();
            Vector clusterProbs = getClusterProbs(newMeansAndClusterProbs);
            List<Vector> newMeans = getMeans(newMeansAndClusterProbs);
            List<Matrix> newCovs = updateCovarianceMatrices(dataset, N, clusterProbs, newMeans);

            List<MultivariateGaussianDistribution> components = buildComponents(newMeans, newCovs);
            GmmModel newModel = new GmmModel(clusterProbs, components);

            countOfIterations++;
            isConverged = isConverged(model, newModel) || countOfIterations > maxCountOfIterations;
            model = newModel;

            if (!isConverged)
                dataset.compute(GmmPartitionData.pcxiUpdater(clusterProbs, components));
        }

        return model;
    }

    private GmmModel init(Dataset<EmptyContext, GmmPartitionData> dataset) {
        List<Vector> randomMeans = dataset.compute(
            selectNRandomXsMapper(countOfComponents),
            selectNRandomXsReducer(countOfComponents, seed)
        );

        int featuresCount = randomMeans.get(0).size();
        List<MultivariateGaussianDistribution> distributions = new ArrayList<>();
        for (int i = 0; i < countOfComponents; i++) {
            distributions.add(new MultivariateGaussianDistribution(
                randomMeans.get(i),
                MatrixUtil.identity(featuresCount)
            ));
        }

        return new GmmModel(
            VectorUtils.of(DoubleStream.generate(() -> 1. / countOfComponents).limit(countOfComponents).toArray()),
            distributions
        );
    }

    private List<MeanWithClusterProbAggregator> updateMeans(Dataset<EmptyContext, GmmPartitionData> dataset) {
        return dataset.compute(
            MeanWithClusterProbAggregator.map(countOfComponents),
            MeanWithClusterProbAggregator::reduce
        );
    }

    private Vector getClusterProbs(List<MeanWithClusterProbAggregator> meansAggr) {
        return VectorUtils.of(meansAggr.stream().mapToDouble(MeanWithClusterProbAggregator::clusterProb).toArray());
    }

    private List<Vector> getMeans(List<MeanWithClusterProbAggregator> meansAggr) {
        return meansAggr.stream().map(MeanWithClusterProbAggregator::mean).collect(Collectors.toList());
    }

    private List<Matrix> updateCovarianceMatrices(Dataset<EmptyContext, GmmPartitionData> dataset, int N,
        Vector clusterProbs, List<Vector> means) {

        List<CovarianceMatricesAggregator> aggregators = dataset.compute(
            CovarianceMatricesAggregator.map(countOfComponents, means),
            CovarianceMatricesAggregator::reduce
        );

        List<Matrix> res = new ArrayList<>();
        for (int i = 0; i < aggregators.size(); i++)
            res.add(aggregators.get(i).covariance(clusterProbs.get(i)));

        return res;
    }

    private List<MultivariateGaussianDistribution> buildComponents(List<Vector> means, List<Matrix> covs) {
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

            isConverged = isConverged && Math.sqrt(d1.mean().getDistanceSquared(d2.mean())) < EPS;
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
        return (data, env) -> env.randomNumbersGenerator().ints(n, 0, data.size()).mapToObj(data::getX)
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
