package org.apache.ignite.ml.optimization;

import java.io.Serializable;
import org.apache.ignite.ml.math.Vector;

/**
 * Weights updater applied on every gradient descent step to decide how weights should be changed.
 */
@FunctionalInterface
public interface Updater extends Serializable {

    /** */
    Vector compute(Vector oldWeights, Vector oldGradient, Vector weights, Vector gradient, int iteration);
}
