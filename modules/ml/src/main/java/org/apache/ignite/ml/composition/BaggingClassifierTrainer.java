package org.apache.ignite.ml.composition;

import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.composition.answercomputer.ModelsCompositionAnswerComputer;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.selection.split.mapper.SHA256UniformMapper;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.util.Utils;

import java.util.*;
import java.util.stream.IntStream;

public abstract class BaggingClassifierTrainer<M extends Model<double[], Double>> implements DatasetTrainer<ModelsComposition<M>, Double> {
    private final ModelsCompositionAnswerComputer modelsCompositionAnswerComputer;
    private final int maximumFeaturesCountPerModel;
    private final int countOfModels;
    private final double samplePartSizePerModel;

    public BaggingClassifierTrainer(ModelsCompositionAnswerComputer modelsCompositionAnswerComputer, int maximumFeaturesCountPerModel, int countOfModels, double samplePartSizePerModel) {
        this.modelsCompositionAnswerComputer = modelsCompositionAnswerComputer;
        this.maximumFeaturesCountPerModel = maximumFeaturesCountPerModel;
        this.countOfModels = countOfModels;
        this.samplePartSizePerModel = samplePartSizePerModel;
    }

    @Override
    public <K, V> ModelsComposition<M> fit(DatasetBuilder<K, V> datasetBuilder,
                                           IgniteBiFunction<K, V, double[]> featureExtractor,
                                           IgniteBiFunction<K, V, Double> lbExtractor) {
        List<ModelsComposition.ModelWithFeatureMapping<M>> learnedModels = new ArrayList<>();

        for(int i = 0; i < countOfModels; i++) {
            final Random rnd = new Random();
            final SHA256UniformMapper<K, V> sampleFilter = new SHA256UniformMapper<>(rnd);
            final long featureExtractorSeed = rnd.nextLong();

            M model = buildDatasetTrainerForModel().fit(
                    datasetBuilder.withFilter((features, answer) -> sampleFilter.map(features, answer) < samplePartSizePerModel),
                    wrapFeatureExtractor(featureExtractor, featureExtractorSeed),
                    lbExtractor);

            Map<Integer, Integer> featuresMapping = createFeaturesMapping(featureExtractorSeed, getFeaturesVectorSize(model));
            learnedModels.add(new ModelsComposition.ModelWithFeatureMapping<>(featuresMapping, model));
        }

        return new ModelsComposition<M>(modelsCompositionAnswerComputer, learnedModels);
    }

    private Map<Integer, Integer> createFeaturesMapping(long featureExtractorSeed, int featuresVectorSize) {
        final int[] featureIdxs = Utils.selectKDistinct(featuresVectorSize,
                maximumFeaturesCountPerModel,
                new Random(featureExtractorSeed)
        );
        final Map<Integer, Integer> localFeaturesMapping = new HashMap<>();
        IntStream.range(0, maximumFeaturesCountPerModel)
                .forEach(localId -> localFeaturesMapping.put(localId, featureIdxs[localId]));

        return localFeaturesMapping;
    }

    protected abstract DatasetTrainer<M, Double> buildDatasetTrainerForModel();

    protected abstract int getFeaturesVectorSize(M model);

    private <K, V> IgniteBiFunction<K, V, double[]> wrapFeatureExtractor(IgniteBiFunction<K, V, double[]> featureExtractor, long featureExtractorSeed) {
        return featureExtractor.andThen((IgniteFunction<double[], double[]>) featureValues -> {
            double[] newFeaturesValues = new double[maximumFeaturesCountPerModel];
            int ptr = 0;
            int[] featureIdxs = Utils.selectKDistinct(featureValues.length,
                    maximumFeaturesCountPerModel,
                    new Random(featureExtractorSeed)
            );
            for (int featureId : featureIdxs) {
                newFeaturesValues[ptr] = featureValues[featureId];
                ptr++;
            }

            return newFeaturesValues;
        });
    }
}
