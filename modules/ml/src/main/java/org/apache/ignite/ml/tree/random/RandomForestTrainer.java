package org.apache.ignite.ml.tree.random;

import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.selection.split.mapper.SHA256UniformMapper;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.tree.DecisionTree;
import org.apache.ignite.ml.tree.DecisionTreeNode;
import org.apache.ignite.ml.tree.impurity.ImpurityMeasure;
import org.apache.ignite.ml.util.Utils;

import java.util.*;
import java.util.stream.IntStream;

public abstract class RandomForestTrainer<T extends ImpurityMeasure<T>> implements DatasetTrainer<RandomForest, Double> {
    private final int treesCount;
    private final double learningBatchSize;
    private final int maximumUsedFeatures;
    protected final int maxDeep;
    protected final int minImpurityDecrease;

    public RandomForestTrainer(int maxDeep, int minImpurityDecrease,
                               int treesCount, double learningBatchSize,
                               int featuresPartSize) {
        this.maxDeep = maxDeep;
        this.minImpurityDecrease = minImpurityDecrease;
        this.treesCount = treesCount;
        this.learningBatchSize = learningBatchSize;
        this.maximumUsedFeatures = featuresPartSize;
    }

    @Override
    public <K, V> RandomForest fit(DatasetBuilder<K, V> datasetBuilder,
                                   IgniteBiFunction<K, V, double[]> featureExtractor,
                                   IgniteBiFunction<K, V, Double> lbExtractor) {
        List<RandomForest.RandomTree> learnedTrees = new ArrayList<>();

        for (int i = 0; i < treesCount; i++) {
            final Random rnd = new Random();
            final SHA256UniformMapper<K, V> sampleFilter = new SHA256UniformMapper<>(rnd);
            final long featureExtractorSeed = rnd.nextLong();

            datasetBuilder = datasetBuilder.withFilter((features, answer) -> {
                return sampleFilter.map(features, answer) < learningBatchSize;
            });

            featureExtractor = featureExtractor.andThen((IgniteFunction<double[], double[]>) featureValues -> {
                double[] newFeaturesValues = new double[maximumUsedFeatures];
                int ptr = 0;
                int[] featureIdxs = Utils.selectKDistinct(featureValues.length,
                        maximumUsedFeatures,
                        new Random(featureExtractorSeed)
                );
                for (int featureId : featureIdxs) {
                    newFeaturesValues[ptr] = featureValues[featureId];
                    ptr++;
                }

                return newFeaturesValues;
            });

            DecisionTreeNode tree = buildDecisionTreeTrainer()
                    .fit(datasetBuilder, featureExtractor, lbExtractor);

            int featuresCount = tree.getFeaturesCount();
            int[] featureIdxs = Utils.selectKDistinct(featuresCount,
                    maximumUsedFeatures,
                    new Random(featureExtractorSeed)
            );
            Map<Integer, Integer> localFeaturesMapping = new HashMap<>();
            IntStream.range(0, maximumUsedFeatures)
                    .forEach(localId -> localFeaturesMapping.put(localId, featureIdxs[localId]));

            learnedTrees.add(new RandomForest.RandomTree(tree, localFeaturesMapping));
        }

        return new RandomForest(learnedTrees, getAnswerCalculator());
    }

    protected abstract DecisionTree<T> buildDecisionTreeTrainer();

    protected abstract RandomForestAnswerCalculator getAnswerCalculator();
}
