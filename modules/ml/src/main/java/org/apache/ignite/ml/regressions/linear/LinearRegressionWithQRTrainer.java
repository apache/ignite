package org.apache.ignite.ml.regressions.linear;

import org.apache.ignite.ml.Trainer;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.decompositions.QRDSolver;
import org.apache.ignite.ml.math.decompositions.QRDecomposition;
import org.apache.ignite.ml.math.impls.vector.FunctionVector;

/**
 * Linear regression trainer based on least squares loss function and QR decomposition.
 */
public class LinearRegressionWithQRTrainer implements Trainer<LinearRegressionModel, Matrix> {

    /**
     * {@inheritDoc}
     */
    @Override public LinearRegressionModel train(Matrix data) {
        Vector groundTruth = extractGroundTruth(data);
        Matrix inputs = extractInputs(data);
        QRDecomposition decomposition = new QRDecomposition(inputs);
        QRDSolver solver = new QRDSolver(decomposition.getQ(), decomposition.getR());
        Vector variables = solver.solve(groundTruth);
        Vector weights = variables.viewPart(1, variables.size() - 1);
        double intercept = variables.get(0);
        return new LinearRegressionModel(weights, intercept);
    }

    /**
     * Extracts first column with ground truth from the data set matrix.
     *
     * @param data data to build model
     * @return Ground truth vector
     */
    private Vector extractGroundTruth(Matrix data) {
        return data.getCol(0);
    }

    /**
     * Extracts all inputs from data set matrix and updates matrix so that first column contains value 1.0.
     *
     * @param data data to build model
     * @return Inputs matrix
     */
    private Matrix extractInputs(Matrix data) {
        data = data.copy();
        data.assignColumn(0, new FunctionVector(data.rowSize(), row -> 1.0));
        return data;
    }
}
