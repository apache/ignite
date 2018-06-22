package org.apache.ignite.ml.composition;

import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.composition.answercomputer.ModelsCompositionAnswerComputer;

import java.util.List;
import java.util.Map;

public class ModelsComposition<M extends Model<double[], Double>> implements Model<double[], Double> {
    private final ModelsCompositionAnswerComputer modelsCompositionAnswerComputer;
    private final List<ModelWithFeatureMapping<M>> models;

    public ModelsComposition(ModelsCompositionAnswerComputer modelsCompositionAnswerComputer, List<ModelWithFeatureMapping<M>> models) {
        this.modelsCompositionAnswerComputer = modelsCompositionAnswerComputer;
        this.models = models;
    }

    @Override
    public Double apply(double[] features) {
        double[] predictions = new double[models.size()];
        for(int i = 0; i < models.size(); i++)
            predictions[i] = models.get(i).apply(features);

        return modelsCompositionAnswerComputer.apply(predictions);
    }

    static class ModelWithFeatureMapping<M extends Model<double[], Double>> implements Model<double[], Double> {
        private final Map<Integer, Integer> featuresMapping;
        private final M model;

        public ModelWithFeatureMapping(Map<Integer, Integer> featuresMapping, M model) {
            this.featuresMapping = featuresMapping;
            this.model = model;
        }

        @Override
        public Double apply(double[] features) {
            double[] newFeatures = new double[featuresMapping.size()];
            featuresMapping.forEach((localId, featureVectorId) -> newFeatures[localId] = features[featureVectorId]);
            return model.apply(newFeatures);
        }
    }
}
