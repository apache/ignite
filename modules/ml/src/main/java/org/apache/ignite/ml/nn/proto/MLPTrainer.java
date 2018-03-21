package org.apache.ignite.ml.nn.proto;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Random;
import org.apache.ignite.ml.MultiLabelDatasetTrainer;
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
 * @param <P> Type of model update used in this trainer.
 */
public class MLPTrainer<P extends Serializable> implements MultiLabelDatasetTrainer<MultilayerPerceptron> {
    /** Multilayer perceptron architecture that defines layers and activators. */
    private final MLPArchitecture arch;

    /** Loss function to be minimized during the training. */
    private final IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss;

    /** Update strategy that defines how to update model parameters during the training. */
    private final UpdatesStrategy<? super MultilayerPerceptron, P> updatesStgy;

    /** Error threshold used to stop the training. */
    private final double errorThreshold;

    /** Maximal number of iterations before the training will be stopped. */
    private final int maxIterations;

    /** Batch size (per every partition). */
    private final int batchSize;

    /** Maximal number of local iterations before synchronization. */
    private final int locIterations;

    /** Multilayer perceptron model initializer. */
    private final MLPInitializer initializer;

    /**
     * Constructs a new instance of multilayer perceptron trainer.
     *
     * @param arch Multilayer perceptron architecture that defines layers and activators.
     * @param loss Loss function to be minimized during the training.
     * @param updatesStgy Update strategy that defines how to update model parameters during the training.
     * @param errorThreshold Error threshold used to stop the training.
     * @param maxIterations Maximal number of iterations before the training will be stopped.
     * @param batchSize Batch size (per every partition).
     * @param locIterations Maximal number of local iterations before synchronization.
     * @param initializer Multilayer perceptron model initializer.
     */
    public MLPTrainer(MLPArchitecture arch, IgniteFunction<Vector, IgniteDifferentiableVectorToDoubleFunction> loss,
        UpdatesStrategy<? super MultilayerPerceptron, P> updatesStgy,
        double errorThreshold, int maxIterations, int batchSize, int locIterations, MLPInitializer initializer) {
        this.arch = arch;
        this.loss = loss;
        this.updatesStgy = updatesStgy;
        this.errorThreshold = errorThreshold;
        this.maxIterations = maxIterations;
        this.batchSize = batchSize;
        this.locIterations = locIterations;
        this.initializer = initializer;
    }

    /** {@inheritDoc} */
    public <K, V> MultilayerPerceptron fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, double[]> featureExtractor, IgniteBiFunction<K, V, double[]> lbExtractor) {

        MultilayerPerceptron mdl = new MultilayerPerceptron(arch, initializer);
        ParameterUpdateCalculator<? super MultilayerPerceptron, P> updater = updatesStgy.getUpdatesCalculator();


        try (Dataset<EmptyContext, SimpleLabeledDatasetData> dataset = datasetBuilder.build(
            new EmptyContextBuilder<>(),
            new SimpleLabeledDatasetDataBuilder<>(featureExtractor, lbExtractor)
        )) {

            P updaterParams = updater.init(mdl, loss);

            for (int i = 0; i < maxIterations; i+=locIterations) {
                updater.update(
                    mdl,
                    dataset.compute(
                        data -> {
                            P update = null;
                            for (int j = 0; j < locIterations; j++) {
                                int[] rows = Utils.selectKDistinct(0, Math.min(batchSize, data.getRows()), new Random());
                                Matrix inputs = new DenseLocalOnHeapMatrix(batch(data.getFeatures(), rows, data.getRows()), data.getFeatures().length / data.getRows(), 0);
                                Matrix groundTruth = new DenseLocalOnHeapMatrix(batch(data.getLabels(), rows, data.getRows()), data.getLabels().length / data.getRows(), 0);
                                update = updatesStgy.locStepUpdatesReducer().apply(Arrays.asList(update, updater.calculateNewUpdate(mdl, updaterParams, 0, inputs, groundTruth)));
                            }
                            return update;
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
