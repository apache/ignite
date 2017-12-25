package org.apache.ignite.ml.optimization;

import org.apache.ignite.ml.math.Vector;

/**
 * Simple updater with fixed learning rate which doesn't guarantee convergence.
 */
public class SimpleUpdater implements Updater {

    /** */
    private final double learningRate;

    /** */
    public SimpleUpdater(double learningRate) {
        if (learningRate <= 0) {
            throw new IllegalArgumentException("Learning rate must be positive but got " + learningRate);
        }
        this.learningRate = learningRate;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Vector compute(Vector oldWeights, Vector oldGradient, Vector weights, Vector gradient, int iteration) {
        return weights.minus(gradient.times(learningRate));
    }
}
