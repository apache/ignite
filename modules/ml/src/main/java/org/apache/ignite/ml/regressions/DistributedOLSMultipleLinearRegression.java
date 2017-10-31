package org.apache.ignite.ml.regressions;

import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.decompositions.distributed.DistributedQRDecomposition;
import org.apache.ignite.ml.math.exceptions.MathIllegalArgumentException;
import org.apache.ignite.ml.math.exceptions.SingularMatrixException;
import org.apache.ignite.ml.math.functions.Functions;
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
     * Loads model x and y sample data, overriding any previous sample.
     *
     * Computes and caches QR decomposition of the X matrix.
     *
     * @param y the {@code n}-sized vector representing the y sample
     * @param x the {@code n x k} matrix representing the x sample
     * @throws MathIllegalArgumentException if the x and y array data are not compatible for the regression
     */
    public void newSampleData(Vector y, Matrix x) throws MathIllegalArgumentException { // TODO: move to the parent class
        validateSampleData(x, y);
        newYSampleData(y);
        newXSampleData(x);
    }

    /**
     * {@inheritDoc}
     * <p>This implementation computes and caches the QR decomposition of the X matrix.</p>
     */
    public void newSampleData(double[] data, int nobs, int nvars, Matrix like) {
        super.newSampleData(data, nobs, nvars, like);
        qr = new DistributedQRDecomposition(getX(), threshold);
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

        for (int i = 0; i < n; i++) // DEBUG: can be parallelized in n Ignite threads?
            for (int j = 0; j < n; j++)
                if (i == j && i < p)
                    augI.setX(i, j, 1d);
                else
                    augI.setX(i, j, 0d);

        MatrixUtil.toString("augI", augI, augI.columnSize(), augI.rowSize());
        Matrix times = q.times(augI);
        MatrixUtil.toString("qtimes", times, times.columnSize(), times.rowSize());
        // Compute and return Hat matrix
        // No DME advertised - args valid if we get here
        return q.times(augI).times(q.transpose());
    }

    /**
     * <p>Returns the sum of squared deviations of Y from its mean.</p>
     *
     * <p>If the model has no intercept term, <code>0</code> is used for the
     * mean of Y - i.e., what is returned is the sum of the squared Y values.</p>
     *
     * <p>The value returned by this method is the SSTO value used in
     * the {@link #calculateRSquared() R-squared} computation.</p>
     *
     * @return SSTO - the total sum of squares
     * @throws NullPointerException if the sample has not been set
     * @see #isNoIntercept()
     */
    public double calculateTotalSumOfSquares() {
        if (isNoIntercept())
            return getY().foldMap(Functions.PLUS, Functions.SQUARE, 0.0);
        else {
            // TODO: IGNITE-5826, think about incremental update formula.
            final double mean = getY().sum() / getY().size();
            return getY().foldMap(Functions.PLUS, x -> (mean - x) * (mean - x), 0.0); // TODO: implement foldMap for cachedVector
        }
    }

    /**
     * Returns the sum of squared residuals.
     *
     * @return residual sum of squares
     * @throws SingularMatrixException if the design matrix is singular
     * @throws NullPointerException if the data for the model have not been loaded
     */
    public double calculateResidualSumOfSquares() {
        final Vector residuals = calculateResiduals(); // TODO: should be distributed
        // No advertised DME, args are valid
        return residuals.dot(residuals); //TODO: should be distributed
    }

    /**
     * Returns the R-Squared statistic, defined by the formula <pre>
     * R<sup>2</sup> = 1 - SSR / SSTO
     * </pre>
     * where SSR is the {@link #calculateResidualSumOfSquares() sum of squared residuals}
     * and SSTO is the {@link #calculateTotalSumOfSquares() total sum of squares}
     *
     * <p>If there is no variance in y, i.e., SSTO = 0, NaN is returned.</p>
     *
     * @return R-square statistic
     * @throws NullPointerException if the sample has not been set
     * @throws SingularMatrixException if the design matrix is singular
     */
    public double calculateRSquared() {
        return 1 - calculateResidualSumOfSquares() / calculateTotalSumOfSquares(); // DEBUG: it will be distributed due to distribution of two called methods
    }


    public double calculateAdjustedRSquared() {
        final double n = getX().rowSize();
        if (isNoIntercept())
            return 1 - (1 - calculateRSquared()) * (n / (n - getX().columnSize()));
        else
            return 1 - (calculateResidualSumOfSquares() * (n - 1)) /
                    (calculateTotalSumOfSquares() * (n - getX().columnSize()));
    }

    /**
     * {@inheritDoc}
     * <p>This implementation computes and caches the QR decomposition of the X matrix
     * once it is successfully loaded.</p>
     */
    @Override protected void newXSampleData(Matrix x) {
        super.newXSampleData(x); // TODO: distribute it
        qr = new DistributedQRDecomposition(getX()); // TODO: distribute it
    }



    /**
     * Calculates the regression coefficients using OLS.
     *
     * <p>Data for the model must have been successfully loaded using one of
     * the {@code newSampleData} methods before invoking this method; otherwise
     * a {@code NullPointerException} will be thrown.</p>
     *
     * @return beta
     * @throws SingularMatrixException if the design matrix is singular
     * @throws NullPointerException if the data for the model have not been loaded
     */
    @Override protected Vector calculateBeta() {
        return qr.solve(getY());
    }

    /**
     * <p>Calculates the variance-covariance matrix of the regression parameters.
     * </p>
     * <p>Var(b) = (X<sup>T</sup>X)<sup>-1</sup>
     * </p>
     * <p>Uses QR decomposition to reduce (X<sup>T</sup>X)<sup>-1</sup>
     * to (R<sup>T</sup>R)<sup>-1</sup>, with only the top p rows of
     * R included, where p = the length of the beta vector.</p>
     *
     * <p>Data for the model must have been successfully loaded using one of
     * the {@code newSampleData} methods before invoking this method; otherwise
     * a {@code NullPointerException} will be thrown.</p>
     *
     * @return The beta variance-covariance matrix
     * @throws SingularMatrixException if the design matrix is singular
     * @throws NullPointerException if the data for the model have not been loaded
     */
    @Override protected Matrix calculateBetaVariance() {
        int p = getX().columnSize();

        Matrix rAug = MatrixUtil.copy(qr.getR().viewPart(0, p, 0, p)); //TODO: distribute it
        Matrix rInv = rAug.inverse(); // TODO: distribute it

        return rInv.times(rInv.transpose()); //TODO: distribute it
    }


}
