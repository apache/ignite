package org.apache.ignite.ml.naivebayes.compound;

import org.apache.ignite.ml.common.TrainerTest;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.naivebayes.discrete.DiscreteNaiveBayesModel;
import org.apache.ignite.ml.naivebayes.discrete.DiscreteNaiveBayesTrainer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/** Test for {@link CompoundNaiveBayesTrainer} */
public class CompoundNaiveBayesTrainerTest extends TrainerTest {

    /** Precision in test checks. */
    private static final double PRECISION = 1e-2;

    /** The first label */
    private static final double LABEL_1 = 1.;

    /** The second label */
    private static final double LABEL_2 = 2.;

    private static final Map<Integer, double[]> data = new HashMap<>();

    /** */
    private static final double[][] BINARIZED_DATA_THRESHOLDS = new double[][] {{.5}, {.5}, {.5}, {.5}, {.5}};

    static {
        data.put(0, new double[]{6, 180, 12, 0, 0, 1, 1, 1, LABEL_1});
        data.put(1, new double[]{5.92, 190, 11, 1, 0, 1, 1, 0, LABEL_1});
        data.put(2, new double[]{5.58, 170, 12, 1, 1, 0, 0, 1, LABEL_1});
        data.put(3, new double[]{5.92, 165, 10, 1, 1, 0, 0, 0, LABEL_1});
//        data.put(4, new double[] {0, 1, 0, 0, 1, LABEL_1});
//        data.put(5, new double[] {0, 0, 0, 1, 0, LABEL_1});

        data.put(6, new double[]{5, 100, 6, 1, 0, 0, 1, 1, LABEL_2});
        data.put(7, new double[]{5.5, 150, 8, 1, 1, 0, 0, 1, LABEL_2});
        data.put(8, new double[]{5.42, 130, 7, 1, 1, 1, 1, 0, LABEL_2});
        data.put(9, new double[]{5.75, 150, 9, 1, 1, 0, 1, 0, LABEL_2});
//        data.put(10, new double[] {1, 1, 0, 1, 1, LABEL_2});
//        data.put(11, new double[] {1, 0, 1, 1, 0, LABEL_2});
//        data.put(12, new double[] {1, 0, 1, 0, 0, LABEL_2});

    }

    /** */
    private CompoundNaiveBayesTrainer trainer;

    /** Initialization {@code CompoundNaiveBayesTrainer}. */
    @Before
    public void createTrainer() {
        trainer = new CompoundNaiveBayesTrainer()
                .setLabels(new double[]{LABEL_1, LABEL_2})
                .setClsProbabilities(new double[]{.5, .5})
                .setDiscreteNaiveBayesTrainer(new DiscreteNaiveBayesTrainer()
                        .setBucketThresholds(BINARIZED_DATA_THRESHOLDS)
                        .withEquiprobableClasses()
                        .setSkipFeature(f -> f <= 2));
    }

    @Test /** */
    public void test(){
        CompoundNaiveBayesModel model = trainer.fit(
                new LocalDatasetBuilder<>(data, parts),
                (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 0, v.length - 1)),
                (k, v) -> v[v.length - 1]
        );

        assertDiscreteModel(model.getDiscreteModel());
    }

    private void assertDiscreteModel(DiscreteNaiveBayesModel discreteModel) {
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
                Assert.assertArrayEquals(expectedPriorProbabilites[i][j], discreteModel.getProbabilities()[i][j], PRECISION);
        }
    }
}
