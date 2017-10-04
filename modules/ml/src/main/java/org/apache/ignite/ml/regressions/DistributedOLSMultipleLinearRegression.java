package org.apache.ignite.ml.regressions;

import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.decompositions.QRDecomposition;
import org.apache.ignite.ml.math.decompositions.distributed.DistributedQRDecomposition;
import org.apache.ignite.ml.math.exceptions.MathIllegalArgumentException;


public class DistributedOLSMultipleLinearRegression extends AbstractMultipleLinearRegression {
    /** Cached QR decomposition of X matrix */
    private DistributedQRDecomposition qr = null; // TODO: add QRDecomposition interface or abstract class

    /** Singularity threshold for QR decomposition */
    private final double threshold;

    /**
     * Create an empty OLSMultipleLinearRegression instance.
     */
    public DistributedOLSMultipleLinearRegression() {
        this(0d);
    }



    /**
     * Create an empty OLSMultipleLinearRegression instance, using the given
     * singularity threshold for the QR decomposition.
     *
     * @param threshold the singularity threshold
     */
    public DistributedOLSMultipleLinearRegression(final double threshold) {
        this.threshold = threshold;
    }


    /**
     * {@inheritDoc}
     * <p>This implementation computes and caches the QR decomposition of the X matrix.</p>
     */
    @Override public void newSampleData(double[] data, int nobs, int nvars, Matrix like) {
        super.newSampleData(data, nobs, nvars, like); //DEBUG:  it has distributed Matrix support
        qr = new DistributedQRDecomposition(getX(), threshold); // DEBUG: it will be distributed as promised
    }


    @Override
    protected Vector calculateBeta() {
        return null;
    }

    @Override
    protected Matrix calculateBetaVariance() {
        return null;
    }
}
