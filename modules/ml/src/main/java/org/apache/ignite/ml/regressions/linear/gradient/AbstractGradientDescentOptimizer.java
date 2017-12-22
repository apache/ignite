package org.apache.ignite.ml.regressions.linear.gradient;

import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;

/**
 * Basic implementation of gradient descent optimizer.
 */
public abstract class AbstractGradientDescentOptimizer implements GradientDescentOptimizer {

    /**
     * Function to be optimized.
     */
    private final DifferentiableFunction function;

    /**
     * Termination tolerance for objective function.
     */
    private final double tolerance;

    /**
     * Maximal number of iterations.
     */
    private final double maxIterations;

    /**
     * Current variables which are considered as a point where objective function takes optimal value.
     */
    private Vector vars;

    /**
     * Value of objective function which updates after every iteration of gradient descent.
     */
    private Double prevOFValue;

    /** */
    AbstractGradientDescentOptimizer(DifferentiableFunction function, double tolerance, int maxIterations) {
        this.function = function;
        this.tolerance = tolerance;
        this.maxIterations = maxIterations;
        this.vars = new DenseLocalOnHeapVector(function.getNvars());
    }

    /**
     * {@inheritDoc}
     */
    public Vector optimize(boolean maximize) {
        for (int iteration = 0; iteration < maxIterations; iteration++) {
            vars = makeGradientStep(maximize, iteration);
            Double newOFValue = function.calculate(vars);
            boolean completed = prevOFValue != null && Math.abs(prevOFValue - newOFValue) <= tolerance;
            prevOFValue = newOFValue;
            if (completed)
                break;
        }
        return vars;
    }

    /**
     * Make a gradient step in terms of changing variables so that objective function become smaller or bigger (closer
     * to desired minimal value) depends on {@code maximize} parameter.
     *
     * @param maximize Flag which reflects if we need to maximize function or minimize
     */
    abstract protected Vector makeGradientStep(boolean maximize, int iteration);

    /** */
    protected DifferentiableFunction getFunction() {
        return function;
    }

    /** */
    Vector getVars() {
        return vars;
    }
}