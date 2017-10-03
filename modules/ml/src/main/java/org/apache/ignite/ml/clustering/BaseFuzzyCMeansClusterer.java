package org.apache.ignite.ml.clustering;

import org.apache.ignite.ml.math.DistanceMeasure;
import org.apache.ignite.ml.math.Matrix;

public abstract class BaseFuzzyCMeansClusterer<T extends Matrix> implements Clusterer<T, FuzzyCMeansModel> {
    private DistanceMeasure measure;

    private double exponentialWeight;

    public abstract FuzzyCMeansModel cluster(T points, int k);
}
