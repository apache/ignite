package org.apache.ignite.ml.naivebayes.compound;

import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.naivebayes.discrete.DiscreteNaiveBayesModel;
import org.apache.ignite.ml.naivebayes.discrete.DiscreteNaiveBayesTrainer;
import org.apache.ignite.ml.naivebayes.gaussian.GaussianNaiveBayesModel;
import org.apache.ignite.ml.naivebayes.gaussian.GaussianNaiveBayesTrainer;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static java.util.Arrays.asList;
import static org.apache.ignite.ml.naivebayes.compound.Data.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/** Test for {@link CompoundNaiveBayesTrainer} */
public class CompoundNaiveBayesTrainerTest extends TrainerTest {

    /** Precision in test checks. */
    private static final double PRECISION = 1e-2;

    /** */
    private CompoundNaiveBayesTrainer trainer;

    /** Initialization {@code CompoundNaiveBayesTrainer}. */
    @Before
    public void createTrainer() {
        trainer = new CompoundNaiveBayesTrainer()
                .setLabels(labels)
                .setClsProbabilities(classProbabilities)
                .setGaussianNaiveBayesTrainer(new GaussianNaiveBayesTrainer().setFeatureIdsToSkip(asList(3,4,5,6,7)))
                .setDiscreteNaiveBayesTrainer(new DiscreteNaiveBayesTrainer()
                        .setBucketThresholds(binarizedDataThresholds)
                        .withEquiprobableClasses()
                        .setFeatureIdsToSkip(asList(0,1,2)));
    }

    @Test /** */
    public void test(){
        CompoundNaiveBayesModel model = trainer.fit(
                new LocalDatasetBuilder<>(data, parts),
                (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 0, v.length - 1)),
                (k, v) -> v[v.length - 1]
        );

        assertDiscreteModel(model.getDiscreteModel());
        assertGaussianModel(model.getGaussianModel());
    }

    /** */
    private void assertDiscreteModel(DiscreteNaiveBayesModel model) {
        double[][][] expectedPriorProbabilites = new double[][][]{
                {
                        {.25, .75},
                        {.5, .5},
                        {.5, .5},
                        {.5, .5}
                },
                {
                        {.0, 1},
                        {.25, .75},
                        {.75, .25},
                        {.25, .75},
                }
        };

        for (int i = 0; i < expectedPriorProbabilites.length; i++) {
            for (int j = 0; j < expectedPriorProbabilites[i].length; j++)
                assertArrayEquals(expectedPriorProbabilites[i][j], model.getProbabilities()[i][j], PRECISION);
        }
        assertArrayEquals(new double[] {.5, .5}, model.getClsProbabilities(), PRECISION);
    }

    /** */
    private void assertGaussianModel(GaussianNaiveBayesModel model) {
        double[] priorProbabilities = new double[]{.5, .5};

        assertEquals(priorProbabilities[0], model.getClassProbabilities()[0], PRECISION);
        assertEquals(priorProbabilities[1], model.getClassProbabilities()[1], PRECISION);
        assertArrayEquals(new double[]{5.855, 176.25, 11.25, 0, 0, 0, 0, 0}, model.getMeans()[0], PRECISION);
        assertArrayEquals(new double[]{5.4175, 132.5, 7.5, 0, 0, 0, 0, 0}, model.getMeans()[1], PRECISION);
        double[] expectedVars = {0.026274999999999, 92.1875, 0.6875, 0, 0, 0, 0, 0};
        assertArrayEquals(expectedVars, model.getVariances()[0], PRECISION);
    }
}
