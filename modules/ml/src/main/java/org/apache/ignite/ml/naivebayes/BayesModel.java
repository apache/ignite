package org.apache.ignite.ml.naivebayes;

import org.apache.ignite.ml.Exportable;
import org.apache.ignite.ml.IgniteModel;

/**
 * Interface for Bayes Models.
 */
public interface BayesModel<MODEL extends BayesModel, FEATURES, OUTPUT>
        extends IgniteModel<FEATURES, OUTPUT>, Exportable<MODEL> {

    /**
     * Returns an array where the index correapons a label, and value corresponds {@code log(probalility)} to be this label.
     * The prior probabilities are not count.
     */
    double[] probabilityPowers(FEATURES vector);
}
