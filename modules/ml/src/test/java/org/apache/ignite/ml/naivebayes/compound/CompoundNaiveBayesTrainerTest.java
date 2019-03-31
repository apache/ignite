package org.apache.ignite.ml.naivebayes.compound;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.common.TrainerTest;
import org.junit.Before;

/** Test for {@link CompoundNaiveBayesTrainer} */
public class CompoundNaiveBayesTrainerTest extends TrainerTest {

    /** Precision in test checks. */
    private static final double PRECISION = 1e-2;

    /** The first label */
    private static final double LABEL_1 = 1.;

    /** The second label */
    private static final double LABEL_2 = 2.;

    private static final Map<Integer, double[]> data = new HashMap<>();

    static {
        data.put(0, new double[] {6, 180, 12, 0, 0, 1, 1, 1, LABEL_1});
        data.put(1, new double[] {5.92, 190, 11, 1, 0, 1, 1, 0, LABEL_1});
        data.put(2, new double[] {5.58, 170, 12, 1, 1, 0, 0, 1, LABEL_1});
        data.put(3, new double[] {5.92, 165, 10, 1, 1, 0, 0, 0, LABEL_1});
//        data.put(4, new double[] {0, 1, 0, 0, 1, LABEL_1});
//        data.put(5, new double[] {0, 0, 0, 1, 0, LABEL_1});

        data.put(6, new double[] {5, 100, 6, 1, 0, 0, 1, 1, LABEL_2});
        data.put(7, new double[] {5.5, 150, 8, 1, 1, 0, 0, 1, LABEL_2});
        data.put(8, new double[] {5.42, 130, 7, 1, 1, 1, 1, 0, LABEL_2});
        data.put(9, new double[] {5.75, 150, 9, 1, 1, 0, 1, 0, LABEL_2});
//        data.put(10, new double[] {1, 1, 0, 1, 1, LABEL_2});
//        data.put(11, new double[] {1, 0, 1, 1, 0, LABEL_2});
//        data.put(12, new double[] {1, 0, 1, 0, 0, LABEL_2});

    }

    /** */
    private CompoundNaiveBayesTrainer trainer;

    /** Initialization {@code CompoundNaiveBayesTrainer}. */
    @Before
    public void createTrainer() {
        trainer = new CompoundNaiveBayesTrainer();
    }

}
