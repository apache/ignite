/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.ml.regressions;

import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.decompositions.QRDecomposition;
import org.apache.ignite.ml.math.exceptions.MathIllegalArgumentException;
import org.apache.ignite.ml.math.exceptions.SingularMatrixException;
import org.apache.ignite.ml.math.functions.Functions;
import org.apache.ignite.ml.math.util.MatrixUtil;

/**
 * This class is based on the corresponding class from Apache Common Math lib.
 * <p>Implements ordinary least squares (OLS) to estimate the parameters of a
 * multiple linear regression model.</p>
 *
 * <p>The regression coefficients, <code>b</code>, satisfy the normal equations:
 * <pre><code> X<sup>T</sup> X b = X<sup>T</sup> y </code></pre></p>
 *
 * <p>To solve the normal equations, this implementation uses QR decomposition
 * of the <code>X</code> matrix. (See {@link QRDecomposition} for details on the
 * decomposition algorithm.) The <code>X</code> matrix, also known as the <i>design matrix,</i>
 * has rows corresponding to sample observations and columns corresponding to independent
 * variables.  When the model is estimated using an intercept term (i.e. when
 * {@link #isNoIntercept() isNoIntercept} is false as it is by default), the <code>X</code>
 * matrix includes an initial column identically equal to 1.  We solve the normal equations
 * as follows:
 * <pre><code> X<sup>T</sup>X b = X<sup>T</sup> y
 * (QR)<sup>T</sup> (QR) b = (QR)<sup>T</sup>y
 * R<sup>T</sup> (Q<sup>T</sup>Q) R b = R<sup>T</sup> Q<sup>T</sup> y
 * R<sup>T</sup> R b = R<sup>T</sup> Q<sup>T</sup> y
 * (R<sup>T</sup>)<sup>-1</sup> R<sup>T</sup> R b = (R<sup>T</sup>)<sup>-1</sup> R<sup>T</sup> Q<sup>T</sup> y
 * R b = Q<sup>T</sup> y </code></pre></p>
 *
 * <p>Given <code>Q</code> and <code>R</code>, the last equation is solved by back-substitution.</p>
 */
public class OLSMultipleLinearRegression extends AbstractMultipleLinearRegression {
    /** Cached QR decomposition of X matrix */
    private QRDecomposition qr = null;

    /** Singularity threshold for QR decomposition */
    private final double threshold;

    /**
     * Create an empty OLSMultipleLinearRegression instance.
     */
    public OLSMultipleLinearRegression() {
        this(0d);
    }

    /**
     * Create an empty OLSMultipleLinearRegression instance, using the given
     * singularity threshold for the QR decomposition.
     *
     * @param threshold the singularity threshold
     */
    public OLSMultipleLinearRegression(final double threshold) {
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
    public void newSampleData(Vector y, Matrix x) throws MathIllegalArgumentException {
        validateSampleData(x, y);
        newYSampleData(y);
        newXSampleData(x);
    }

    /**
     * {@inheritDoc}
     * <p>This implementation computes and caches the QR decomposition of the X matrix.</p>
     */
    @Override public void newSampleData(double[] data, int nobs, int nvars, Matrix like) {
        super.newSampleData(data, nobs, nvars, like);
        qr = new QRDecomposition(getX(), threshold);
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
            return getY().foldMap(Functions.PLUS, x -> (mean - x) * (mean - x), 0.0);
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
        final Vector residuals = calculateResiduals();
        // No advertised DME, args are valid
        return residuals.dot(residuals);
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
        return 1 - calculateResidualSumOfSquares() / calculateTotalSumOfSquares();
    }

    /**
     * <p>Returns the adjusted R-squared statistic, defined by the formula <pre>
     * R<sup>2</sup><sub>adj</sub> = 1 - [SSR (n - 1)] / [SSTO (n - p)]
     * </pre>
     * where SSR is the {@link #calculateResidualSumOfSquares() sum of squared residuals},
     * SSTO is the {@link #calculateTotalSumOfSquares() total sum of squares}, n is the number
     * of observations and p is the number of parameters estimated (including the intercept).</p>
     *
     * <p>If the regression is estimated without an intercept term, what is returned is <pre>
     * <code> 1 - (1 - {@link #calculateRSquared()}) * (n / (n - p)) </code>
     * </pre></p>
     *
     * <p>If there is no variance in y, i.e., SSTO = 0, NaN is returned.</p>
     *
     * @return adjusted R-Squared statistic
     * @throws NullPointerException if the sample has not been set
     * @throws SingularMatrixException if the design matrix is singular
     * @see #isNoIntercept()
     */
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
        super.newXSampleData(x);
        qr = new QRDecomposition(getX());
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

        Matrix rAug = MatrixUtil.copy(qr.getR().viewPart(0, p, 0, p));
        Matrix rInv = rAug.inverse();

        return rInv.times(rInv.transpose());
    }
}
