package org.apache.ignite.ml.regressions.linear.gradient;

import org.apache.ignite.ml.math.Vector;

/**
 * Simple (non-stochastic) gradient descent which uses the same learning rate at every step. Be aware that convergence
 * in this method is not guaranteed.
 */
public class GradientDescentOptimizerWithFixedLearningRate extends AbstractGradientDescentOptimizer {

    /**
     * Learning Rate used on every gradient descent step to choose distance to move.
     */
    private final double learningRate;

    /** */
    public GradientDescentOptimizerWithFixedLearningRate(DifferentiableFunction function, double tolerance,
        int maxIterations, double learningRate) {
        super(function, tolerance, maxIterations);
        this.learningRate = learningRate;
    }

    /**
     * {@inheritDoc}
     */
    @Override protected Vector makeGradientStep(boolean maximize, int iteration) {
        Vector gradient = getFunction().gradient(getVars());
        Vector weightsDelta = gradient.times(learningRate).times(maximize ? 1.0 : -1.0);
        return getVars().plus(weightsDelta);
    }
}
