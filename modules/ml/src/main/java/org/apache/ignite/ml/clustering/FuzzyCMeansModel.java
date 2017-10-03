package org.apache.ignite.ml.clustering;

import org.apache.ignite.ml.math.DistanceMeasure;
import org.apache.ignite.ml.math.Vector;

import java.util.Arrays;

public class FuzzyCMeansModel implements ClusterizationModel<Vector, Integer> {
    /** Centers of clusters. */
    private Vector[] centers;

    /** Distance measure. */
    private DistanceMeasure measure;

    public FuzzyCMeansModel(Vector[] centers, DistanceMeasure measure) {
        this.centers = Arrays.copyOf(centers, centers.length);
        this.measure = measure;
    }

    /** @inheritDoc */
    @Override public int clustersCount() {
        return centers.length;
    }

    /** @inheritDoc */
    @Override public Vector[] centers() {
        return Arrays.copyOf(centers, centers.length);
    }

    /** Predict closest center index for a given vector.
     *
     * @param val Vector.
     * @return index of the closest center or -1 if it can't be found
     */
    @Override public Integer predict(Vector val) {
        int index = -1;
        double minDistance = Double.POSITIVE_INFINITY;

        for (int i = 0; i < centers.length; i++) {
            double currentDistance = measure.compute(val, centers[i]);
            if (currentDistance < minDistance) {
                minDistance = currentDistance;
                index = i;
            }
        }

        return index;
    }
}
