package org.apache.ignite.ml.nn.proto;

import java.util.Arrays;
import java.util.Random;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.data.SimpleLabeledDatasetDataBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.dataset.primitive.data.SimpleLabeledDatasetData;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteDifferentiableVectorToDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.nn.MultilayerPerceptron;
import org.apache.ignite.ml.nn.architecture.MLPArchitecture;
import org.apache.ignite.ml.nn.initializers.MLPInitializer;
import org.apache.ignite.ml.optimization.updatecalculators.ParameterUpdateCalculator;
import org.apache.ignite.ml.trainers.group.UpdatesStrategy;
import org.apache.ignite.ml.util.Utils;

/**
 * Multilayer perceptron trainer based on partition based {@link Dataset}.
 *
 * @param <K> Type of a key in <tt>upstream</tt> data.
 * @param <V> Type of a value in <tt>upstream</tt> data.
 * @param <P> Type of model update used in this trainer.
 */
public class MLPTrainer<K, V, P> {

    private final MLPArchitecture arch;

    private final IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss;

    private final UpdatesStrategy<? super MultilayerPerceptron, P> updatesStgy;

    private final double errorThreshold;

    private final int maxIterations;

    private final int batchSize;

    private final MLPInitializer initializer;

    public MLPTrainer(MLPArchitecture arch, IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss,
        UpdatesStrategy<? super MultilayerPerceptron, P> updatesStgy,
        double errorThreshold, int maxIterations, int batchSize, MLPInitializer initializer) {
        this.arch = arch;
        this.loss = loss;
        this.updatesStgy = updatesStgy;
        this.errorThreshold = errorThreshold;
        this.maxIterations = maxIterations;
        this.batchSize = batchSize;
        this.initializer = initializer;
    }

    public MultilayerPerceptron fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, double[]> featureExtractor, IgniteBiFunction<K, V, double[]> lbExtractor) {

        MultilayerPerceptron mdl = new MultilayerPerceptron(arch, initializer);
        ParameterUpdateCalculator<? super MultilayerPerceptron, P> updater = updatesStgy.getUpdatesCalculator();


        try (Dataset<EmptyContext, SimpleLabeledDatasetData> dataset = datasetBuilder.build(
            new EmptyContextBuilder<>(),
            new SimpleLabeledDatasetDataBuilder<>(featureExtractor, lbExtractor)
        )) {

            P updaterParams = updater.init(mdl, loss);

            for (int i = 0; i < maxIterations; i++) {
                updater.update(
                    mdl,
                    dataset.compute(
                        data -> {
                            int[] rows = Utils.selectKDistinct(0, Math.min(batchSize, data.getRows()), new Random());
                            Matrix inputs = new DenseLocalOnHeapMatrix(batch(data.getFeatures(), rows, data.getRows()), data.getFeatures().length / data.getRows(), 0);
                            Matrix groundTruth = new DenseLocalOnHeapMatrix(batch(data.getLabels(), rows, data.getRows()), data.getLabels().length / data.getRows(), 0);
                            return updater.calculateNewUpdate(mdl, updaterParams, 0, inputs, groundTruth);
                        },
                        (a, b) -> a == null ? b : updatesStgy.allUpdatesReducer().apply(Arrays.asList(a, b))
                    )
                );
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        return mdl;
    }

    private double[] batch(double[] data, int[] rows, int totalRows) {
        int cols = data.length / totalRows;
        double[] res = new double[cols * rows.length];
        for (int i = 0; i < rows.length; i++)
            System.arraycopy(data, rows[i] * cols, res, i * cols, cols);
        return res;
    }
}
