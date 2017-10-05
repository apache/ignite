package org.apache.ignite.ml.regressions;

import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.decompositions.QRDecomposition;
import org.apache.ignite.ml.math.decompositions.distributed.DistributedQRDecomposition;
import org.apache.ignite.ml.math.exceptions.MathIllegalArgumentException;
import org.apache.ignite.ml.math.util.MatrixUtil;


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
        super.newSampleData(data, nobs, nvars, like); // DEBUG:  it has distributed Matrix support
        qr = new DistributedQRDecomposition(getX(), threshold); // DEBUG: it should be distributed
    }

    /**
     * <p>Compute the "hat" matrix.
     * </p>
     * <p>The hat matrix is defined in terms of the design matrix X
     * by X(X<sup>T</sup>X)<sup>-1</sup>X<sup>T</sup>
     * </p>
     * <p>The implementation here uses the QR decomposition to compute the
     * hat matrix as Q I<sub>p</sub>Q<sup>T</sup> where I<sub>p</sub> is the
     * p-dimensional identity matrix augmented by 0's.  This computational
     * formula is from "The Hat Matrix in Regression and ANOVA",
     * David C. Hoaglin and Roy E. Welsch,
     * <i>The American Statistician</i>, Vol. 32, No. 1 (Feb., 1978), pp. 17-22.
     * </p>
     * <p>Data for the model must have been successfully loaded using one of
     * the {@code newSampleData} methods before invoking this method; otherwise
     * a {@code NullPointerException} will be thrown.</p>
     *
     * @return the hat matrix
     * @throws NullPointerException unless method {@code newSampleData} has been called beforehand.
     */
    public Matrix calculateHat() {
        // Create augmented identity matrix
        // No try-catch or advertised NotStrictlyPositiveException - NPE above if n < 3
        Matrix q = qr.getQ();
        Matrix augI = MatrixUtil.like(q, q.columnSize(), q.columnSize());

        int n = augI.columnSize();
        int p = qr.getR().columnSize();

        for (int i = 0; i < n; i++)
            for (int j = 0; j < n; j++)
                if (i == j && i < p)
                    augI.setX(i, j, 1d);
                else
                    augI.setX(i, j, 0d);

        // Compute and return Hat matrix
        // No DME advertised - args valid if we get here
        return q.times(augI).times(q.transpose());
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
