package org.apache.ignite.ml.naivebayes.compound;

import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.naivebayes.discrete.DiscreteNaiveBayesTrainer;
import org.apache.ignite.ml.naivebayes.gaussian.GaussianNaiveBayesTrainer;
import org.junit.Test;

import java.util.Arrays;

import static java.util.Arrays.asList;
import static org.apache.ignite.ml.naivebayes.compound.Data.*;
import static org.junit.Assert.assertEquals;

/** Integration tests for Compound naive Bayes algorithm with different datasets. */
public class CompoundNaiveBayesTest {

    /** Precision in test checks. */
    private static final double PRECISION = 1e-2;

    @Test
    public void testLearnsAndPredictCorrently() {
        CompoundNaiveBayesTrainer trainer = new CompoundNaiveBayesTrainer()
                .setLabels(labels)
                .setClsProbabilities(classProbabilities)
                .setGaussianNaiveBayesTrainer(new GaussianNaiveBayesTrainer().setFeatureIdsToSkip(asList(3,4,5,6,7)))
                .setDiscreteNaiveBayesTrainer(new DiscreteNaiveBayesTrainer()
                        .setBucketThresholds(binarizedDataThresholds)
                        .withEquiprobableClasses()
                        .setFeatureIdsToSkip(asList(0,1,2)));

        CompoundNaiveBayesModel model = trainer.fit(
                new LocalDatasetBuilder<>(data, 2),
                (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 0, v.length - 1)),
                (k, v) -> v[v.length - 1]
        );

        Vector observation1 = VectorUtils.of(5.92, 165, 10, 1, 1, 0, 0, 0);
        assertEquals(LABEL_1, model.predict(observation1), PRECISION);

        Vector observation2 = VectorUtils.of(6, 130, 8, 1, 0, 1, 1, 0);
        assertEquals(LABEL_2, model.predict(observation2), PRECISION);
    }
}
