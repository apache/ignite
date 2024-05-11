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
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.environment.logging.MLLogger;
import org.apache.ignite.ml.math.exceptions.math.SingularMatrixException;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.math.stat.MultivariateGaussianDistribution;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.structures.DatasetRow;
import org.apache.ignite.ml.trainers.SingleLabelDatasetTrainer;
import org.jetbrains.annotations.NotNull;

/**
 * Traner for GMM model.
 */
public class GmmTrainer extends SingleLabelDatasetTrainer<GmmModel> {
    /** Min divergence of mean vectors beween iterations. If divergence will less then trainer stops. */
    private double eps = 1e-3;

    /** Count of components. */
    private int countOfComponents = 2;

    /** Max count of iterations. */
    private int maxCountOfIterations = 10;

    /** Initial means. */
    private Vector[] initialMeans;

    /** Maximum initialization tries count. */
    private int maxCountOfInitTries = 3;

    /**
     * Maximum count of clusters that can be achieved.
     */
    private int maxCountOfClusters = 2;

    /** Maximum divergence between maximum of likelihood of vector in dataset and other for anomalies identification. */
    private double maxLikelihoodDivergence = 5;

    /** Minimum required anomalies in terms of maxLikelihoodDivergence for creating new cluster. */
    private double minElementsForNewCluster = 300;

    /** Min cluster probability. */
    private double minClusterProbability = 0.05;

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

    /**
     * Creates an instance of GmmTrainer.
     *
     * @param countOfComponents Count of components.
     */
    public GmmTrainer(int countOfComponents) {
        this.countOfComponents = countOfComponents;
    }

    /** {@inheritDoc} */
    @Override public <K, V> GmmModel fitWithInitializedDeployingContext(DatasetBuilder<K, V> datasetBuilder,
        Preprocessor<K, V> extractor) {
        return updateModel(null, datasetBuilder, extractor);
    }

    /**
     * Returns mapper for initial means selection.
     *
     * @param n Number of components.
     * @return Mapper.
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
     * @return Reducer.
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

    /**
     * Sets numberOfComponents.
     *
     * @param numberOfComponents Number of components.
     * @return Trainer.
     */
    public GmmTrainer withInitialCountOfComponents(int numberOfComponents) {
        A.ensure(numberOfComponents > 0, "Number of components in GMM cannot equal 0");

        this.countOfComponents = numberOfComponents;
        initialMeans = null;
        if (countOfComponents > maxCountOfClusters)
            maxCountOfClusters = countOfComponents;
        return this;
    }

    /**
     * Sets initial means.
     *
     * @param means Initial means for clusters.
     * @return Trainer.
     */
    public GmmTrainer withInitialMeans(List<Vector> means) {
        A.notEmpty(means, "GMM should start with non empty initial components list");

        this.initialMeans = means.toArray(new Vector[means.size()]);
        this.countOfComponents = means.size();
        if (countOfComponents > maxCountOfClusters)
            maxCountOfClusters = countOfComponents;
        return this;
    }

    /**
     * Sets max count of iterations
     *
     * @param maxCountOfIterations Max count of iterations.
     * @return Trainer.
     */
    public GmmTrainer withMaxCountIterations(int maxCountOfIterations) {
        A.ensure(maxCountOfIterations > 0, "Max count iterations cannot be less or equal zero or negative");

        this.maxCountOfIterations = maxCountOfIterations;
        return this;
    }

    /**
     * Sets min divergence beween iterations.
     *
     * @param eps Eps.
     * @return Trainer.
     */
    public GmmTrainer withEps(double eps) {
        A.ensure(eps > 0 && eps < 1.0, "Min divergence beween iterations should be between 0.0 and 1.0");

        this.eps = eps;
        return this;
    }

    /**
     * Sets MaxCountOfInitTries parameter. If means initialization were unsuccessful then algorithm try to reinitialize
     * means randomly MaxCountOfInitTries times.
     *
     * @param maxCountOfInitTries Max count of init tries.
     * @return Trainer.
     */
    public GmmTrainer withMaxCountOfInitTries(int maxCountOfInitTries) {
        A.ensure(maxCountOfInitTries > 0, "Max initialization count should be great than zero.");

        this.maxCountOfInitTries = maxCountOfInitTries;
        return this;
    }

    /**
     * Sets maximum number of clusters in GMM.
     *
     * @param maxCountOfClusters Max count of clusters.
     * @return Trainer.
     */
    public GmmTrainer withMaxCountOfClusters(int maxCountOfClusters) {
        A.ensure(maxCountOfClusters >= countOfComponents, "Max count of components should be greater than " +
            "initial count of components or equal to it");

        this.maxCountOfClusters = maxCountOfClusters;
        return this;
    }

    /**
     * Sets maximum divergence between maximum of likelihood of vector in dataset and other for anomalies
     * identification.
     *
     * @param maxLikelihoodDivergence Max likelihood divergence.
     * @return Trainer.
     */
    public GmmTrainer withMaxLikelihoodDivergence(double maxLikelihoodDivergence) {
        A.ensure(maxLikelihoodDivergence > 0, "Max likelihood divergence should be > 0");

        this.maxLikelihoodDivergence = maxLikelihoodDivergence;
        return this;
    }

    /**
     * Trains model based on the specified data.
     *
     * @param dataset Dataset.
     * @return GMM model.
     */
    private Optional<GmmModel> fit(Dataset<EmptyContext, GmmPartitionData> dataset) {
        return init(dataset).map(model -> {
            GmmModel currentModel = model;

            do {
                UpdateResult updateResult = updateModel(dataset, currentModel);
                currentModel = updateResult.model;

                double minCompProb = currentModel.componentsProbs().minElement().get();
                if (countOfComponents >= maxCountOfClusters || minCompProb < minClusterProbability)
                    break;

                double maxXProb = updateResult.maxProbInDataset;
                NewComponentStatisticsAggregator newMeanAdder = NewComponentStatisticsAggregator.computeNewMean(dataset,
                    maxXProb, maxLikelihoodDivergence, currentModel);

                Vector newMean = newMeanAdder.mean();
                if (newMeanAdder.rowCountForNewCluster() < minElementsForNewCluster)
                    break;

                countOfComponents += 1;
                Vector[] newMeans = new Vector[countOfComponents];
                for (int i = 0; i < currentModel.countOfComponents(); i++)
                    newMeans[i] = currentModel.distributions().get(i).mean();
                newMeans[countOfComponents - 1] = newMean;

                initialMeans = newMeans;

                Optional<GmmModel> newModelOpt = init(dataset);
                if (newModelOpt.isPresent())
                    currentModel = newModelOpt.get();
                else
                    break;
            }
            while (true);

            return filterModel(currentModel);
        });
    }

    /**
     * Sets minimum required anomalies in terms of maxLikelihoodDivergence for creating new cluster.
     *
     * @param minElementsForNewCluster Min elements for new cluster.
     * @return Trainer.
     */
    public GmmTrainer withMinElementsForNewCluster(int minElementsForNewCluster) {
        A.ensure(minElementsForNewCluster > 0, "Min elements for new cluster should be > 0");

        this.minElementsForNewCluster = minElementsForNewCluster;
        return this;
    }

    /**
     * Sets minimum requred probability for cluster. If cluster has probability value less than this value then this
     * cluster will be eliminated.
     *
     * @param minClusterProbability Min cluster probability.
     * @return Trainer.
     */
    public GmmTrainer withMinClusterProbability(double minClusterProbability) {
        this.minClusterProbability = minClusterProbability;
        return this;
    }

    /**
     * Result of current model update by EM-algorithm.
     */
    private static class UpdateResult {
        /** Model. */
        private final GmmModel model;

        /** Max likelihood in dataset. */
        private final double maxProbInDataset;

        /**
         * @param model Model.
         * @param maxProbInDataset Max likelihood in dataset.
         */
        public UpdateResult(GmmModel model, double maxProbInDataset) {
            this.model = model;
            this.maxProbInDataset = maxProbInDataset;
        }
    }

    /**
     * Remove clusters with probability value < minClusterProbability
     *
     * @param model Model.
     * @return Filtered model.
     */
    private GmmModel filterModel(GmmModel model) {
        List<Double> componentProbs = new ArrayList<>();
        List<MultivariateGaussianDistribution> distributions = new ArrayList<>();

        Vector originalComponentProbs = model.componentsProbs();
        List<MultivariateGaussianDistribution> originalDistr = model.distributions();
        for (int i = 0; i < model.countOfComponents(); i++) {
            double prob = originalComponentProbs.get(i);
            if (prob > minClusterProbability) {
                componentProbs.add(prob);
                distributions.add(originalDistr.get(i));
            }
        }

        return new GmmModel(
            VectorUtils.of(componentProbs.toArray(new Double[0])),
            distributions
        );
    }

    /**
     * Gets older model and returns updated model on given data.
     *
     * @param dataset Dataset.
     * @param model Model.
     * @return Updated model.
     */
    @NotNull private UpdateResult updateModel(Dataset<EmptyContext, GmmPartitionData> dataset, GmmModel model) {
        boolean isConverged = false;
        int countOfIterations = 0;
        double maxProbInDataset = Double.NEGATIVE_INFINITY;
        while (!isConverged) {
            MeanWithClusterProbAggregator.AggregatedStats stats = MeanWithClusterProbAggregator.aggreateStats(dataset, countOfComponents);
            Vector clusterProbs = stats.clusterProbabilities();
            Vector[] newMeans = stats.means().toArray(new Vector[countOfComponents]);

            A.ensure(newMeans.length == model.countOfComponents(), "newMeans.size() == count of components");
            A.ensure(newMeans[0].size() == initialMeans[0].size(), "newMeans[0].size() == initialMeans[0].size()");
            List<Matrix> newCovs = CovarianceMatricesAggregator.computeCovariances(dataset, clusterProbs, newMeans);

            try {
                List<MultivariateGaussianDistribution> components = buildComponents(newMeans, newCovs);
                GmmModel newModel = new GmmModel(clusterProbs, components);

                countOfIterations += 1;
                isConverged = isConverged(model, newModel) || countOfIterations > maxCountOfIterations;
                model = newModel;
                maxProbInDataset = GmmPartitionData.updatePcxiAndComputeLikelihood(dataset, clusterProbs, components);
            }
            catch (SingularMatrixException | IllegalArgumentException e) {
                String msg = "Cannot construct non-singular covariance matrix by data. " +
                    "Try to select other initial means or other model trainer. Iterations will stop.";
                environment.logger().log(MLLogger.VerboseLevel.HIGH, msg);
                isConverged = true;
            }
        }

        return new UpdateResult(model, maxProbInDataset);
    }

    /**
     * Init means and covariances.
     *
     * @param dataset Dataset.
     * @return Initial model.
     */
    private Optional<GmmModel> init(Dataset<EmptyContext, GmmPartitionData> dataset) {
        int cntOfTries = 0;

        while (true) {
            try {
                if (initialMeans == null) {
                    List<Vector> randomMeansSets = Stream.of(dataset.compute(
                        selectNRandomXsMapper(countOfComponents),
                        GmmTrainer::selectNRandomXsReducer))
                        .flatMap(Stream::of)
                        .sorted(Comparator.comparingDouble(Vector::getLengthSquared)).collect(Collectors.toList());

                    Collections.shuffle(randomMeansSets, environment.randomNumbersGenerator());

                    A.ensure(
                        randomMeansSets.size() >= countOfComponents,
                        "There is not enough data in dataset for select N random means"
                    );

                    initialMeans = randomMeansSets.subList(0, countOfComponents)
                        .toArray(new Vector[countOfComponents]);
                }

                dataset.compute(data -> GmmPartitionData.estimateLikelihoodClusters(data, initialMeans));

                List<Matrix> initialCovs = CovarianceMatricesAggregator.computeCovariances(
                    dataset,
                    VectorUtils.fill(1. / countOfComponents, countOfComponents),
                    initialMeans
                );

                if (initialCovs.isEmpty())
                    return Optional.empty();

                List<MultivariateGaussianDistribution> distributions = new ArrayList<>();
                for (int i = 0; i < countOfComponents; i++)
                    distributions.add(new MultivariateGaussianDistribution(initialMeans[i], initialCovs.get(i)));

                return Optional.of(new GmmModel(
                    VectorUtils.of(DoubleStream.generate(() -> 1. / countOfComponents).limit(countOfComponents).toArray()),
                    distributions
                ));
            }
            catch (SingularMatrixException | IllegalArgumentException e) {
                String msg = "Cannot construct non-singular covariance matrix by data. " +
                    "Try to select other initial means or other model trainer [number of tries = " + cntOfTries + "]";
                environment.logger().log(MLLogger.VerboseLevel.HIGH, msg);
                cntOfTries += 1;
                initialMeans = null;
                if (cntOfTries >= maxCountOfInitTries)
                    throw new RuntimeException(msg, e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isUpdateable(GmmModel mdl) {
        return mdl.countOfComponents() == countOfComponents;
    }

    /** {@inheritDoc} */
    @Override protected <K, V> GmmModel updateModel(GmmModel mdl, DatasetBuilder<K, V> datasetBuilder,
        Preprocessor<K, V> extractor) {

        try (Dataset<EmptyContext, GmmPartitionData> dataset = datasetBuilder.build(envBuilder,
            new EmptyContextBuilder<>(),
            new GmmPartitionData.Builder<>(extractor, maxCountOfClusters),
            learningEnvironment()
        )) {
            if (mdl != null) {
                if (initialMeans != null)
                    environment.logger().log(MLLogger.VerboseLevel.HIGH, "Initial means will be replaced by model from update");
                initialMeans = mdl.distributions().stream()
                    .map(MultivariateGaussianDistribution::mean)
                    .toArray(Vector[]::new);
            }

            Optional<GmmModel> model = fit(dataset);
            if (model.isPresent())
                return model.get();
            else if (mdl != null)
                return mdl;
            else
                throw new IllegalArgumentException("Cannot learn model on empty dataset.");
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Create new model components with provided means and covariances.
     *
     * @param means Means.
     * @param covs Covariances.
     * @return Gmm components.
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
     * @return True if algorithm gonverged.
     */
    private boolean isConverged(GmmModel oldModel, GmmModel newModel) {
        A.ensure(oldModel.countOfComponents() == newModel.countOfComponents(),
            "oldModel.countOfComponents() == newModel.countOfComponents()");

        for (int i = 0; i < oldModel.countOfComponents(); i++) {
            MultivariateGaussianDistribution d1 = oldModel.distributions().get(i);
            MultivariateGaussianDistribution d2 = newModel.distributions().get(i);

            if (Math.sqrt(d1.mean().getDistanceSquared(d2.mean())) >= eps)
                return false;
        }

        return true;
    }
}
