package org.apache.ignite.ml.optimization;

import java.io.Serializable;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;

/**
 * Function which computes gradient of the loss function at any given point.
 */
@FunctionalInterface
public interface GradientFunction extends Serializable {

    /** */
    Vector compute(Matrix inputs, Vector groundTruth, Vector point);
}
