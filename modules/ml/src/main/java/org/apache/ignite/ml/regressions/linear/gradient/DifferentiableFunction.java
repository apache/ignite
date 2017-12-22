package org.apache.ignite.ml.regressions.linear.gradient;

import org.apache.ignite.ml.math.Vector;

/**
 * Function which has gradient and can be used in gradient descent optimizer.
 */
public interface DifferentiableFunction {

    /**
     * Calculates value of the function in specified point.
     *
     * @param vars Variables
     * @return Value of the function
     */
    double calculate(Vector vars);

    /**
     * Calculates gradient of the function in specified point.
     *
     * @param vars Variables
     * @return Gradient of the function
     */
    Vector gradient(Vector vars);

    /**
     * Returns number of variables.
     *
     * @return Number of variables
     */
    int getNvars();
}
