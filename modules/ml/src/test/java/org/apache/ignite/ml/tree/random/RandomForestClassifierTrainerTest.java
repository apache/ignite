package org.apache.ignite.ml.tree.random;

import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.answercomputer.OnMajorityPredictionsAggregator;
import org.apache.ignite.ml.tree.DecisionTreeNode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class RandomForestClassifierTrainerTest {
    /**
     * Number of parts to be tested.
     */
    private static final int[] partsToBeTested = new int[]{1, 2, 3, 4, 5, 7};

    /**
     * Number of partitions.
     */
    @Parameterized.Parameter
    public int parts;

    @Parameterized.Parameters(name = "Data divided on {0} partitions")
    public static Iterable<Integer[]> data() {
        List<Integer[]> res = new ArrayList<>();
        for (int part : partsToBeTested)
            res.add(new Integer[]{part});

        return res;
    }

    @Test
    public void testFit() {
        int sampleSize = 1000;
        Map<double[], Double> sample = new HashMap<>();
        for (int i = 0; i < sampleSize; i++) {
            double x1 = i;
            double x2 = x1 / 10.0;
            double x3 = x2 / 10.0;
            double x4 = x3 / 10.0;

            sample.put(new double[]{x1, x2, x3, x4}, (double)(i % 2));
        }

        RandomForestClassifierTrainer trainer = new RandomForestClassifierTrainer(4,3, 5, 0.3, 4, 0.1);
        ModelsComposition<DecisionTreeNode> model = trainer.fit(sample, parts, (k, v) -> k, (k, v) -> v);

        assertTrue(model.getPredictionsAggregator() instanceof OnMajorityPredictionsAggregator);
        assertEquals(5, model.getModels().size());

        for(ModelsComposition.ModelOnFeaturesSubspace<DecisionTreeNode> tree : model.getModels())
            assertEquals(3, tree.getFeaturesMapping().size());
    }
}
