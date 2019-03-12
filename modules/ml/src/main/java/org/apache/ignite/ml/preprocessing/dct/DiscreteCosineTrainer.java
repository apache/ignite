package org.apache.ignite.ml.preprocessing.dct;

import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.preprocessing.PreprocessingTrainer;

public class DiscreteCosineTrainer<K, V> implements PreprocessingTrainer<K, V, Vector, Vector> {

    @Override
    public IgniteBiFunction<K, V, Vector> fit(LearningEnvironmentBuilder envBuilder, DatasetBuilder<K, V> datasetBuilder, IgniteBiFunction<K, V, Vector> basePreprocessor) {
        throw new UnsupportedOperationException("Not implemented");
    }
}
