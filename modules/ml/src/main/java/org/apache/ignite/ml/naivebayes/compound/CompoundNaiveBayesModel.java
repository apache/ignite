package org.apache.ignite.ml.naivebayes.compound;

import java.io.Serializable;
import org.apache.ignite.ml.Exportable;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.naivebayes.discrete.DiscreteNaiveBayesModel;
import org.apache.ignite.ml.naivebayes.gaussian.GaussianNaiveBayesModel;

/** Created by Ravil on 04/02/2019. */
public class CompoundNaiveBayesModel<K,V> implements IgniteModel<Vector, Double>, Exportable<CompoundNaiveBayesModel>, Serializable {


    DiscreteNaiveBayesModel discreteNaiveBayesModel;
    IgniteBiFunction<K, V, Vector> discreteFeatureExtractor;
    GaussianNaiveBayesModel gaussianNaiveBayesModel;
    IgniteBiFunction<K, V, Vector> gaussianFeatureExtractor;

    /** {@inheritDoc} */
    @Override public <P> void saveModel(Exporter<CompoundNaiveBayesModel, P> exporter, P path) {
        exporter.save(this, path);
    }

    @Override public Double predict(Vector input) {

        return null;
    }
}
