package org.apache.ignite.ml.pipeline;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.preprocessing.PreprocessingTrainer;
import org.apache.ignite.ml.trainers.DatasetTrainer;

public class Pipeline<K, V> {

    private IgniteBiFunction<K, V, Object[]> finalFeatureExtractor;

    private IgniteBiFunction<K, V, Double> lbExtractor;

    private List<PreprocessingTrainer> preprocessors = new ArrayList<>();

    private DatasetTrainer finalStage;

    public Pipeline<K, V> addFeatureExtractor(IgniteBiFunction<K, V, Object[]> featureExtractor) {
        this.finalFeatureExtractor = featureExtractor;
        return this;
    }

    public Pipeline<K, V> addLabelExtractor(IgniteBiFunction<K, V, Double> lbExtractor) {
        this.lbExtractor = lbExtractor;
        return this;
    }

    public Pipeline<K, V> addStage(PreprocessingTrainer trainer) {
        preprocessors.add(trainer);
        return this;
    }

    public Pipeline<K, V> addFinalStage(DatasetTrainer trainer) {
        this.finalStage = trainer;
        return this;
    }

    public PipelineMdl fit(Ignite ignite, IgniteCache<K, V> cache) {
        assert lbExtractor != null;
        assert finalFeatureExtractor != null;

        preprocessors.forEach(e -> {
            finalFeatureExtractor = e.fit(ignite,
                cache,
                finalFeatureExtractor
            );
        });


        Model internalMdl = finalStage
            .fit(
                ignite,
                cache,
                finalFeatureExtractor,
                lbExtractor
            );
        return new PipelineMdl()
            .withFeatureExtractor(finalFeatureExtractor)
            .withLabelExtractor(lbExtractor)
            .withInternalMdl(internalMdl);
    }
}
