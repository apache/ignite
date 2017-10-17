package org.apache.ignite.ml.clustering;

import org.apache.ignite.ml.math.DistanceMeasure;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;

public abstract class BaseFuzzyCMeansClusterer<T extends Matrix> implements Clusterer<T, FuzzyCMeansModel> {
    protected DistanceMeasure measure;

    protected double exponentialWeight;
    protected double maxCentersDelta;

    public abstract FuzzyCMeansModel cluster(T points, int k);

    /**
     * Calculates the distance between two vectors. * with the configured {@link DistanceMeasure}.
     *
     * @return the distance between the two clusterables
     */
    protected double distance(final Vector v1, final Vector v2) {
        return measure.compute(v1, v2);
    }
}
