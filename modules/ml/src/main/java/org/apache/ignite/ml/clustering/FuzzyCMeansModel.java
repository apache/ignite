package org.apache.ignite.ml.clustering;

import org.apache.ignite.ml.Exportable;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.FuzzyCMeansModelFormat;
import org.apache.ignite.ml.math.DistanceMeasure;
import org.apache.ignite.ml.math.Vector;

import java.util.Arrays;

public class FuzzyCMeansModel implements ClusterizationModel<Vector, Integer>, Exportable<FuzzyCMeansModelFormat> {
    /** Centers of clusters. */
    private Vector[] centers;

    /** Distance measure. */
    private DistanceMeasure measure;

    /**
     * Constructor that creates FCM model by centers and measure.
     *
     * @param centers Array of centers.
     * @param measure Distance measure.
     */
    public FuzzyCMeansModel(Vector[] centers, DistanceMeasure measure) {
        this.centers = Arrays.copyOf(centers, centers.length);
        this.measure = measure;
    }

    /** Distance measure used while clusterization. */
    public DistanceMeasure distanceMeasure() {
        return measure;
    }

    /** @inheritDoc */
    @Override public int clustersCount() {
        return centers.length;
    }

    /** @inheritDoc */
    @Override public Vector[] centers() {
        return Arrays.copyOf(centers, centers.length);
    }

    /**
     * Predict closest center index for a given vector.
     *
     * @param val Vector.
     * @return Index of the closest center or -1 if it can't be found.
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

    /** {@inheritDoc} */
    @Override public <P> void saveModel(Exporter<FuzzyCMeansModelFormat, P> exporter, P path) {
        FuzzyCMeansModelFormat modelData = new FuzzyCMeansModelFormat(centers, measure);

        exporter.save(modelData, path);
    }
}
