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
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.environment.logging.MLLogger;
import org.apache.ignite.ml.math.exceptions.SingularMatrixException;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.math.stat.MultivariateGaussianDistribution;
import org.apache.ignite.ml.structures.DatasetRow;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.trainers.FeatureLabelExtractor;

/**
 * Traner for GMM model.
 */
public class GmmTrainer extends DatasetTrainer<GmmModel, Double> {
    /** Min divergence of mean vectors beween iterations. If divergence will less then trainer stops. */
    private double eps = 1e-3;

    /** Count of components. */
    private int countOfComponents = 2;

    /** Max count of iterations. */
    private int maxCountOfIterations = 10;

    /** Initial means. */
    private Vector[] initialMeans;

    /**
     * Creates an instance of GmmTrainer.
     */
    public GmmTrainer() {
    }

    /**
     * Creates an instance of GmmTrainer.
     *
     * @param countOfComponents Count of components.
     * @param maxCountOfIterations Max count of iterations.
     */
    public GmmTrainer(int countOfComponents, int maxCountOfIterations) {
        this.countOfComponents = countOfComponents;
        this.maxCountOfIterations = maxCountOfIterations;
    }

    /** {@inheritDoc} */
    @Override
    public <K, V> GmmModel fit(DatasetBuilder<K, V> datasetBuilder, FeatureLabelExtractor<K, V, Double> extractor) {
        return updateModel(null, datasetBuilder, extractor);
    }

    /**
     * Sets numberOfComponents.
     *
     * @param numberOfComponents Number of components.
     * @return trainer.
     */
    public GmmTrainer withCountOfComponents(int numberOfComponents) {
        A.ensure(numberOfComponents > 0, "Number of components in GMM cannot equal 0");

        this.countOfComponents = numberOfComponents;
        initialMeans = null;
        return this;
    }

    /**
     * Sets initial means.
     *
     * @param means Initial means for clusters.
     * @return trainer.
     */
    public GmmTrainer withInitialMeans(List<Vector> means) {
        A.notEmpty(means, "GMM should starts with non empty initial components list");

        this.initialMeans = means.stream().toArray(Vector[]::new);
        this.countOfComponents = means.size();
        return this;
    }

    /**
     * Sets max count of iterations
     *
     * @param maxCountOfIterations Max count of iterations.
     * @return trainer.
     */
    public GmmTrainer withMaxCountIterations(int maxCountOfIterations) {
        A.ensure(maxCountOfIterations > 0, "Max count iterations cannot be less or equal zero");

        this.maxCountOfIterations = maxCountOfIterations;
        return this;
    }

    /**
     * Sets min divergence beween iterations.
     *
     * @param eps Eps.
     * @return trainer.
     */
    public GmmTrainer withEps(double eps) {
        A.ensure(eps > 0 && eps < 1.0, "Min divergence beween iterations should be between 0.0 and 1.0");

        this.eps = eps;
        return this;
    }

    /**
     * Trains model based on the specified data.
     *
     * @param dataset Dataset.
     * @return GMM model.
     */
    private GmmModel fit(Dataset<EmptyContext, GmmPartitionData> dataset) {
        GmmModel model = init(dataset);
        boolean isConverged = false;
        int countOfIterations = 0;
        while (!isConverged) {
            MeanWithClusterProbAggregator.AggregatedStats stats = MeanWithClusterProbAggregator.aggreateStats(dataset);
            Vector clusterProbs = stats.clusterProbabilities();
            Vector[] newMeans = stats.means().stream().toArray(Vector[]::new);

            A.ensure(newMeans.length == model.countOfComponents(), "newMeans.size() == count of components");
            A.ensure(newMeans[0].size() == initialMeans[0].size(), "newMeans[0].size() == initialMeans[0].size()");
            List<Matrix> newCovs = CovarianceMatricesAggregator.computeCovariances(dataset, clusterProbs, newMeans);

            try {
                List<MultivariateGaussianDistribution> components = buildComponents(newMeans, newCovs);
                GmmModel newModel = new GmmModel(clusterProbs, components);

                countOfIterations += 1;
                isConverged = isConverged(model, newModel) || countOfIterations > maxCountOfIterations;
                model = newModel;

                if (!isConverged)
                    dataset.compute(data -> GmmPartitionData.updatePcxi(data, clusterProbs, components));
            }
            catch (SingularMatrixException | IllegalArgumentException e) {
                String msg = "Cannot construct non-singular covariance matrix by data. " +
                    "Try to select other initial means or other model trainer. Iterations will stop.";
                environment.logger().log(MLLogger.VerboseLevel.HIGH, msg);
                isConverged = true;
            }
        }

        return model;
    }

    /**
     * Init means and covariances.
     *
     * @param dataset Dataset.
     * @return initial model.
     */
    private GmmModel init(Dataset<EmptyContext, GmmPartitionData> dataset) {
        try {
            if (initialMeans == null) {
                List<List<Vector>> randomMeansSets = Stream.of(dataset.compute(
                    selectNRandomXsMapper(countOfComponents),
                    GmmTrainer::selectNRandomXsReducer
                )).map(Arrays::asList).collect(Collectors.toList());

                A.ensure(
                    randomMeansSets.stream().mapToInt(List::size).sum() >= countOfComponents,
                    "There is not enough data in dataset for select N random means"
                );

                initialMeans = new Vector[countOfComponents];
                int j = 0;
                for (int i = 0; i < countOfComponents; ) {
                    List<Vector> randomMeansPart = randomMeansSets.get(j);
                    if (!randomMeansPart.isEmpty()) {
                        initialMeans[i] = randomMeansPart.remove(0);
                        i++;
                    }

                    j = (j + 1) % randomMeansSets.size();
                }
            }

            dataset.compute(data -> GmmPartitionData.estimateLikelihoodClusters(data, initialMeans));

            List<Matrix> initialCovs = CovarianceMatricesAggregator.computeCovariances(
                dataset,
                VectorUtils.of(DoubleStream.generate(() -> 1. / countOfComponents).limit(countOfComponents).toArray()),
                initialMeans
            );

            List<MultivariateGaussianDistribution> distributions = new ArrayList<>();
            for (int i = 0; i < countOfComponents; i++)
                distributions.add(new MultivariateGaussianDistribution(initialMeans[i], initialCovs.get(i)));

            return new GmmModel(
                VectorUtils.of(DoubleStream.generate(() -> 1. / countOfComponents).limit(countOfComponents).toArray()),
                distributions
            );
        }
        catch (SingularMatrixException | IllegalArgumentException e) {
            String msg = "Cannot construct non-singular covariance matrix by data. " +
                "Try to select other initial means or other model trainer";
            environment.logger().log(MLLogger.VerboseLevel.HIGH, msg);
            throw new RuntimeException(msg, e);
        }
    }

    /**
     * Create new model components with provided means and covariances.
     *
     * @param means Means.
     * @param covs Covariances.
     * @return gmm components.
     */
    private List<MultivariateGaussianDistribution> buildComponents(Vector[] means, List<Matrix> covs) {
        A.ensure(means.length == covs.size(), "means.size() == covs.size()");

        List<MultivariateGaussianDistribution> res = new ArrayList<>();
        for (int i = 0; i < means.length; i++)
            res.add(new MultivariateGaussianDistribution(means[i], covs.get(i)));

        return res;
    }

    /**
     * Check algorithm covergency. If it's true then algorithm stops.
     *
     * @param oldModel Old model.
     * @param newModel New model.
     */
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

    /** {@inheritDoc} */
    @Override public boolean isUpdateable(GmmModel mdl) {
        return mdl.countOfComponents() == countOfComponents;
    }

    /** {@inheritDoc} */
    @Override protected <K, V> GmmModel updateModel(GmmModel mdl, DatasetBuilder<K, V> datasetBuilder,
        FeatureLabelExtractor<K, V, Double> extractor) {

        try (Dataset<EmptyContext, GmmPartitionData> dataset = datasetBuilder.build(
            LearningEnvironmentBuilder.defaultBuilder(),
            new EmptyContextBuilder<>(),
            new GmmPartitionData.Builder<>(extractor, countOfComponents)
        )) {
            if (mdl != null) {
                if (initialMeans != null)
                    environment.logger().log(MLLogger.VerboseLevel.HIGH, "Initial means will be replaced by model from update");
                initialMeans = mdl.distributions().stream()
                    .map(MultivariateGaussianDistribution::mean)
                    .toArray(Vector[]::new);
            }

            return fit(dataset);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns mapper for initial means selection.
     *
     * @param n Number of components.
     * @return mapper.
     */
    private static IgniteBiFunction<GmmPartitionData, LearningEnvironment, Vector[][]> selectNRandomXsMapper(int n) {
        return (data, env) -> {
            Vector[] result;

            if (data.size() <= n) {
                result = data.getAllXs().stream()
                    .map(DatasetRow::features)
                    .toArray(Vector[]::new);
            }
            else {
                result = env.randomNumbersGenerator().ints(0, data.size())
                    .distinct().mapToObj(data::getX).limit(n)
                    .toArray(Vector[]::new);
            }

            return new Vector[][] {result};
        };
    }

    /**
     * Reducer for means selection.
     *
     * @return reducer.
     */
    private static Vector[][] selectNRandomXsReducer(Vector[][] l, Vector[][] r) {
        A.ensure(l != null || r != null, "l != null || r != null");

        if (l == null)
            return r;
        if (r == null)
            return l;

        Vector[][] res = new Vector[l.length + r.length][];
        System.arraycopy(l, 0, res, 0, l.length);
        System.arraycopy(r, 0, res, l.length, r.length);

        return res;
    }
}
