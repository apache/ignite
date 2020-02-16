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

package org.apache.ignite.ml.selection.cv;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.environment.parallelism.Promise;
import org.apache.ignite.ml.math.functions.IgniteDoubleConsumer;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.pipeline.Pipeline;
import org.apache.ignite.ml.pipeline.PipelineMdl;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.selection.paramgrid.BruteForceStrategy;
import org.apache.ignite.ml.selection.paramgrid.EvolutionOptimizationStrategy;
import org.apache.ignite.ml.selection.paramgrid.HyperParameterTuningStrategy;
import org.apache.ignite.ml.selection.paramgrid.ParamGrid;
import org.apache.ignite.ml.selection.paramgrid.ParameterSetGenerator;
import org.apache.ignite.ml.selection.paramgrid.RandomStrategy;
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.apache.ignite.ml.selection.scoring.metric.Metric;
import org.apache.ignite.ml.selection.scoring.metric.MetricName;
import org.apache.ignite.ml.selection.split.mapper.SHA256UniformMapper;
import org.apache.ignite.ml.selection.split.mapper.UniformMapper;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.util.genetic.Chromosome;
import org.apache.ignite.ml.util.genetic.GeneticAlgorithm;
import org.jetbrains.annotations.NotNull;

/**
 * Cross validation score calculator. Cross validation is an approach that allows to avoid overfitting that is made the
 * following way: the training set is split into k smaller sets. The following procedure is followed for each of the k
 * “folds”:
 * <ul>
 * <li>A model is trained using k-1 of the folds as training data;</li>
 * <li>the resulting model is validated on the remaining part of the data (i.e., it is used as a test set to compute
 * a performance measure such as accuracy).</li>
 * </ul>
 *
 * @param <M> Type of model.
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public abstract class AbstractCrossValidation<M extends IgniteModel<Vector, Double>, K, V> implements Serializable {
    /** Learning environment builder. */
    protected LearningEnvironmentBuilder envBuilder = LearningEnvironmentBuilder.defaultBuilder();

    /** Learning Environment. */
    protected LearningEnvironment environment = envBuilder.buildForTrainer();

    /** Trainer. */
    protected DatasetTrainer<M, Double> trainer;

    /** Pipeline. */
    protected Pipeline<K, V, Integer, Double> pipeline;

    /** Metric. */
    protected Metric metric;

    /** Preprocessor. */
    protected Preprocessor<K, V> preprocessor;

    /** Filter. */
    protected IgniteBiPredicate<K, V> filter = (k, v) -> true;

    /** Amount of folds. */
    protected int amountOfFolds;

    /** Parts. */
    protected int parts;

    /** Parameter grid. */
    protected ParamGrid paramGrid;

    /** Execution over the pipeline or the chain of preprocessors and separate trainer, otherwise. */
    protected boolean isRunningOnPipeline = true;

    /** Mapper. */
    protected UniformMapper<K, V> mapper = new SHA256UniformMapper<>();

    /**
     * Finds the best set of hyper-parameters based on parameter search strategy.
     */
    public CrossValidationResult tuneHyperParameters() {
        HyperParameterTuningStrategy hyperParamTuningStgy = paramGrid.getHyperParameterTuningStrategy();

        if (hyperParamTuningStgy instanceof BruteForceStrategy) return scoreBruteForceHyperparameterOptimization();
        if (hyperParamTuningStgy instanceof RandomStrategy) return scoreRandomSearchHyperparameterOptimization();
        if (hyperParamTuningStgy instanceof EvolutionOptimizationStrategy)
            return scoreEvolutionAlgorithmSearchHyperparameterOptimization();
        else throw new UnsupportedOperationException("This strategy is not supported yet [strategy="
            + paramGrid.getHyperParameterTuningStrategy().getName() + "]");
    }

    /**
     * Finds the best set of hyper-parameters based on Genetic Programming approach.
     */
    private CrossValidationResult scoreEvolutionAlgorithmSearchHyperparameterOptimization() {
        EvolutionOptimizationStrategy stgy = (EvolutionOptimizationStrategy) paramGrid.getHyperParameterTuningStrategy();

        List<Double[]> paramSets = new ParameterSetGenerator(paramGrid.getParamValuesByParamIdx()).generate();

        // initialization
        List<Double[]> paramSetsCp = new ArrayList<>(paramSets);
        Collections.shuffle(paramSetsCp, new Random(stgy.getSeed()));

        int sizeOfPopulation = 20;
        List<Double[]> rndParamSets = paramSetsCp.subList(0, sizeOfPopulation);

        CrossValidationResult cvRes = new CrossValidationResult();

        Function<Chromosome, Double> fitnessFunction = (Chromosome chromosome) -> {
            TaskResult tr = calculateScoresForFixedParamSet(chromosome.toDoubleArray());

            cvRes.addScores(tr.locScores, tr.paramMap);

            final double locAvgScore = Arrays.stream(tr.locScores).average().orElse(Double.MIN_VALUE);

            if (locAvgScore >= cvRes.getBestAvgScore()) {
                cvRes.setBestScore(tr.locScores);
                cvRes.setBestHyperParams(tr.paramMap);
            }

            return locAvgScore;
        };

        Random rnd = new Random(stgy.getSeed()); //TODO: common seed for shared lambdas can produce the same value on each function call? or sequent?

        BiFunction<Integer, Double, Double> mutator = (Integer geneIdx, Double geneValue) -> {
            Double newGeneVal;

            Double[] possibleGeneValues = paramGrid.getParamRawData().get(geneIdx);
            newGeneVal = possibleGeneValues[rnd.nextInt(possibleGeneValues.length)];

            return newGeneVal;
        };

        GeneticAlgorithm ga = new GeneticAlgorithm(rndParamSets);
        ga.withFitnessFunction(fitnessFunction)
            .withMutationOperator(mutator)
            .withAmountOfEliteChromosomes(stgy.getAmountOfEliteChromosomes())
            .withCrossingoverProbability(stgy.getCrossingoverProbability())
            .withCrossoverStgy(stgy.getCrossoverStgy())
            .withAmountOfGenerations(stgy.getAmountOfGenerations())
            .withSelectionStgy(stgy.getSelectionStgy())
            .withMutationProbability(stgy.getMutationProbability());

        if (environment.parallelismStrategy().getParallelism() > 1)
            ga.runParallel(environment);
        else
            ga.run();

        return cvRes;
    }

    /**
     * Finds the best set of hyperparameters based on Random Serach.
     */
    private CrossValidationResult scoreRandomSearchHyperparameterOptimization() {
        RandomStrategy stgy = (RandomStrategy) paramGrid.getHyperParameterTuningStrategy();

        List<Double[]> paramSets = new ParameterSetGenerator(paramGrid.getParamValuesByParamIdx()).generate();

        List<Double[]> paramSetsCp = new ArrayList<>(paramSets);
        Collections.shuffle(paramSetsCp, new Random(stgy.getSeed()));

        CrossValidationResult cvRes = new CrossValidationResult();

        List<Double[]> rndParamSets = paramSetsCp.subList(0, stgy.getMaxTries());

        List<IgniteSupplier<TaskResult>> tasks = rndParamSets.stream()
            .map(paramSet -> (IgniteSupplier<TaskResult>) (() -> calculateScoresForFixedParamSet(paramSet)))
            .collect(Collectors.toList());

        List<TaskResult> taskResults = environment.parallelismStrategy().submit(tasks).stream()
            .map(Promise::unsafeGet)
            .collect(Collectors.toList());

        taskResults.forEach(tr -> {
            cvRes.addScores(tr.locScores, tr.paramMap);

            final double locAvgScore = Arrays.stream(tr.locScores).average().orElse(Double.MIN_VALUE);

            if (locAvgScore >= cvRes.getBestAvgScore()) {
                cvRes.setBestScore(tr.locScores);
                cvRes.setBestHyperParams(tr.paramMap);
            }
        });

        return cvRes;
    }

    /**
     * Finds the best set of hyperparameters based on brute force approach .
     */
    private CrossValidationResult scoreBruteForceHyperparameterOptimization() {
        List<Double[]> paramSets = new ParameterSetGenerator(paramGrid.getParamValuesByParamIdx()).generate();

        CrossValidationResult cvRes = new CrossValidationResult();

        List<IgniteSupplier<TaskResult>> tasks = paramSets.stream()
            .map(paramSet -> (IgniteSupplier<TaskResult>) (() -> calculateScoresForFixedParamSet(paramSet)))
            .collect(Collectors.toList());

        List<TaskResult> taskResults = environment.parallelismStrategy().submit(tasks).stream()
            .map(Promise::unsafeGet)
            .collect(Collectors.toList());

        taskResults.forEach(tr -> {
            cvRes.addScores(tr.locScores, tr.paramMap);

            final double locAvgScore = Arrays.stream(tr.locScores).average().orElse(Double.MIN_VALUE);

            if (locAvgScore > cvRes.getBestAvgScore()) {
                cvRes.setBestScore(tr.locScores);
                cvRes.setBestHyperParams(tr.paramMap);
            }

        });
        return cvRes;
    }

    /**
     * Represents the scores and map of parameters.
     */
    public static class TaskResult {
        /** Parameter map. */
        private Map<String, Double> paramMap;

        /** Local scores. */
        private double[] locScores;

        /**
         * @param paramMap  Parameter map.
         * @param locScores Locale scores.
         */
        public TaskResult(Map<String, Double> paramMap, double[] locScores) {
            this.paramMap = paramMap;
            this.locScores = locScores;
        }

        /**
         * @param paramMap Parameter map.
         */
        public void setParamMap(Map<String, Double> paramMap) {
            this.paramMap = paramMap;
        }

        /**
         * @param locScores Local scores.
         */
        public void setLocScores(double[] locScores) {
            this.locScores = locScores;
        }
    }

    /**
     * Calculates scores by folds and wrap it to TaskResult object.
     *
     * @param paramSet Parameter set.
     */
    private TaskResult calculateScoresForFixedParamSet(Double[] paramSet) {
        Map<String, Double> paramMap = injectAndGetParametersFromPipeline(paramGrid, paramSet);

        double[] locScores = scoreByFolds();

        return new TaskResult(paramMap, locScores);
    }

    /**
     * Calculates score by folds.
     */
    public abstract double[] scoreByFolds();

    /**
     * Forms the parameter map from parameter grid and parameter set.
     *
     * @param paramGrid Parameter grid.
     * @param paramSet  Parameter set.
     */
    @NotNull private Map<String, Double> injectAndGetParametersFromPipeline(ParamGrid paramGrid, Double[] paramSet) {
        Map<String, Double> paramMap = new HashMap<>();

        for (int paramIdx = 0; paramIdx < paramSet.length; paramIdx++) {
            IgniteDoubleConsumer setter = paramGrid.getSetterByIndex(paramIdx);

            Double paramVal = paramSet[paramIdx];
            setter.accept(paramVal);

            paramMap.put(paramGrid.getParamNameByIndex(paramIdx), paramVal);

        }
        return paramMap;
    }

    /**
     * Computes cross-validated metrics.
     *
     * @param datasetBuilderSupplier Dataset builder supplier.
     * @return Array of scores of the estimator for each run of the cross validation.
     */
    protected double[] score(Function<IgniteBiPredicate<K, V>, DatasetBuilder<K, V>> datasetBuilderSupplier) {
        double[] scores = new double[amountOfFolds];

        double foldSize = 1.0 / amountOfFolds;
        for (int i = 0; i < amountOfFolds; i++) {
            double from = foldSize * i;
            double to = foldSize * (i + 1);

            IgniteBiPredicate<K, V> trainSetFilter = (k, v) -> {
                double pnt = mapper.map(k, v);
                return pnt < from || pnt > to;
            };

            IgniteBiPredicate<K, V> testSetFilter = (k, v) -> !trainSetFilter.apply(k, v);

            DatasetBuilder<K, V> trainSet = datasetBuilderSupplier.apply(trainSetFilter);
            M mdl = trainer.fit(trainSet, preprocessor);

            DatasetBuilder<K, V> testSet = datasetBuilderSupplier.apply(testSetFilter);
            scores[i] = Evaluator.evaluate(testSet, mdl, preprocessor, metric).getSingle();
        }

        return scores;
    }

    /**
     * Computes cross-validated metrics.
     *
     * @param datasetBuilderSupplier Dataset builder supplier.
     * @return Array of scores of the estimator for each run of the cross validation.
     */
    protected double[] scorePipeline(Function<IgniteBiPredicate<K, V>, DatasetBuilder<K, V>> datasetBuilderSupplier) {
        double[] scores = new double[amountOfFolds];

        double foldSize = 1.0 / amountOfFolds;
        for (int i = 0; i < amountOfFolds; i++) {
            double from = foldSize * i;
            double to = foldSize * (i + 1);

            IgniteBiPredicate<K, V> trainSetFilter = (k, v) -> {
                double pnt = mapper.map(k, v);
                return pnt < from || pnt > to;
            };

            IgniteBiPredicate<K, V> testSetFilter = (k, v) -> !trainSetFilter.apply(k, v);

            DatasetBuilder<K, V> trainSet = datasetBuilderSupplier.apply(trainSetFilter);
            PipelineMdl<K, V> mdl = pipeline.fit(trainSet);

            DatasetBuilder<K, V> testSet = datasetBuilderSupplier.apply(testSetFilter);
            scores[i] = Evaluator.evaluate(testSet, mdl, pipeline.getFinalPreprocessor(), metric).getSingle();
        }

        return scores;
    }

    /**
     * @param trainer Trainer.
     */
    public AbstractCrossValidation<M, K, V> withTrainer(DatasetTrainer<M, Double> trainer) {
        this.trainer = trainer;
        return this;
    }

    /**
     * @param metric Metric.
     */
    public AbstractCrossValidation<M, K, V> withMetric(MetricName metric) {
        this.metric = metric.create();
        return this;
    }

    /**
     * @param preprocessor Preprocessor.
     */
    public AbstractCrossValidation<M, K, V> withPreprocessor(Preprocessor<K, V> preprocessor) {
        this.preprocessor = preprocessor;
        return this;
    }

    /**
     * @param filter Filter.
     */
    public AbstractCrossValidation<M, K, V> withFilter(IgniteBiPredicate<K, V> filter) {
        this.filter = filter;
        return this;
    }

    /**
     * @param amountOfFolds Amount of folds.
     */
    public AbstractCrossValidation<M, K, V> withAmountOfFolds(int amountOfFolds) {
        this.amountOfFolds = amountOfFolds;
        return this;
    }

    /**
     * @param paramGrid Parameter grid.
     */
    public AbstractCrossValidation<M, K, V> withParamGrid(ParamGrid paramGrid) {
        this.paramGrid = paramGrid;
        return this;
    }

    /**
     * @param runningOnPipeline Running on pipeline.
     */
    public AbstractCrossValidation<M, K, V> isRunningOnPipeline(boolean runningOnPipeline) {
        isRunningOnPipeline = runningOnPipeline;
        return this;
    }

    /**
     * Changes learning Environment.
     *
     * @param envBuilder Learning environment builder.
     */
    // TODO: IGNITE-10441 Think about more elegant ways to perform fluent API.
    public AbstractCrossValidation<M, K, V> withEnvironmentBuilder(LearningEnvironmentBuilder envBuilder) {
        this.envBuilder = envBuilder;
        environment = envBuilder.buildForTrainer();

        return this;
    }

    /**
     * @param pipeline Pipeline.
     */
    public AbstractCrossValidation<M, K, V> withPipeline(Pipeline<K, V, Integer, Double> pipeline) {
        this.pipeline = pipeline;
        return this;
    }

    /**
     * @param mapper Mapper.
     */
    public AbstractCrossValidation<M, K, V> withMapper(UniformMapper<K, V> mapper) {
        this.mapper = mapper;
        return this;
    }
}
