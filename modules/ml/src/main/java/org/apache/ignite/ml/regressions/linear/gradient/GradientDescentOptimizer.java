package org.apache.ignite.ml.regressions.linear.gradient;

import org.apache.ignite.ml.math.Vector;

/**
 * Basic interface for gradient descent based optimization algorithms.
 */
public interface GradientDescentOptimizer {

    /**
     * Returns a vector of variables so that objective function takes the maximal (if parameter {@code maximize} is
     * {@code true}) value or minimal (if parameter {@code maximize} is false) value.
     *
     * @param maximize Flag which reflects if we need to maximize function or minimize
     * @return Variables
     */
    Vector optimize(boolean maximize);

    /**
     * Returns vector of variables so that objective function takes the maximal value.
     *
     * @return Variables
     */
    default Vector maximize() {
        return optimize(true);
    }

    /**
     * Returns a vector of variables so that objective function takes the minimal value.
     *
     * @return Variables
     */
    default Vector minimize() {
        return optimize(false);
    }
}