package org.apache.ignite.ml.nn;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.nn.Activators;
import org.apache.ignite.ml.nn.MLPTrainer;
import org.apache.ignite.ml.nn.MultilayerPerceptron;
import org.apache.ignite.ml.nn.architecture.MLPArchitecture;
import org.apache.ignite.ml.nn.initializers.RandomInitializer;
import org.apache.ignite.ml.optimization.LossFunctions;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDUpdateCalculator;
import org.apache.ignite.ml.trainers.group.UpdatesStrategy;
import org.junit.Test;

public class MLPTrainerTest {

    @Test
    public void testTest() {
        Map<Integer, double[]> data = new HashMap<>();

        Random rnd = new Random(10);
        for (int i = 0; i < 10000; i++) {
            double q = rnd.nextDouble() * 10 - 5;
            data.put(i, new double[]{ q, q > 0 ? 1.0 : 0.0 });
        }

        MLPArchitecture arch = new MLPArchitecture(1)
            .withAddedLayer(5, false, Activators.RELU)
            .withAddedLayer(1, false, Activators.SIGMOID);

        MLPTrainer<SimpleGDParameterUpdate> trainer = new MLPTrainer<>(
            arch,
            LossFunctions.MSE,
            new UpdatesStrategy<>(
                new SimpleGDUpdateCalculator().withLearningRate(0.001),
                SimpleGDParameterUpdate::sumLocal,
                SimpleGDParameterUpdate::avg
            ),
            0,
            10000,
            10,
            5,
            new RandomInitializer(10)
        );

        MultilayerPerceptron mlp = trainer.fit(
            new LocalDatasetBuilder<>(data, 1),
            (k, v) -> Arrays.copyOfRange(v, 0, v.length - 1),
            (k, v) -> Arrays.copyOfRange(v, v.length - 1, v.length)
        );

        Matrix res = mlp.apply(new DenseLocalOnHeapMatrix(new double[][]{
            {10.0, -10.0}
        }));

        for (int i = 0; i < res.rowSize(); i++) {
            for (int j = 0; j < res.columnSize(); j++)
                System.out.print(res.get(i, j) + " ");
            System.out.println();
        }
    }
}
