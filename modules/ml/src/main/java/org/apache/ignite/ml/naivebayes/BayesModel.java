package org.apache.ignite.ml.naivebayes;

import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Interface for Bayes Models.
 */
public interface BayesModel {

    /**
     * Returns an array where the index correapons a label, and value corresponds probalility to be this label. The
     * prior probabilities are not count.
     */
    double[] probabilityPowers(Vector vector);
}
