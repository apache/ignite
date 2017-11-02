package org.apache.ignite.ml;

import org.apache.ignite.ml.math.DistanceMeasure;
import org.apache.ignite.ml.math.Vector;

import java.io.Serializable;
import java.util.Arrays;

/** Fuzzy C-Means model representation */
public class FuzzyCMeansModelFormat implements Serializable {
    /** Centers of clusters. */
    private final Vector[] centers;

    /** Distance measure. */
    private final DistanceMeasure measure;

    /** */
    public FuzzyCMeansModelFormat(Vector[] centers, DistanceMeasure measure) {
        this.centers = centers;
        this.measure = measure;
    }

    /** */
    public DistanceMeasure getMeasure() {
        return measure;
    }

    /** */
    public Vector[] getCenters() {
        return centers;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 1;

        res = res * 37 + measure.hashCode();
        res = res * 37 + Arrays.hashCode(centers);

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null || getClass() != obj.getClass())
            return false;

        FuzzyCMeansModelFormat that = (FuzzyCMeansModelFormat) obj;

        return measure.equals(that.measure) && Arrays.deepEquals(centers, that.centers);
    }
}
