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
import org.apache.ignite.ml.math.exceptions.*;
import org.apache.ignite.ml.math.functions.Functions;
import org.apache.ignite.ml.math.util.MatrixUtil;

/**
 * This class is based on the corresponding class from Apache Common Math lib.
 * Abstract base class for implementations of MultipleLinearRegression.
 */
public abstract class AbstractMultipleLinearRegression implements MultipleLinearRegression {
    /** X sample data. */
    private Matrix xMatrix;

    /** Y sample data. */
    private Vector yVector;

    /** Whether or not the regression model includes an intercept.  True means no intercept. */
    private boolean noIntercept = false;

    /**
     * @return the X sample data.
     */
    protected Matrix getX() {
        return xMatrix;
    }

    /**
     * @return the Y sample data.
     */
    protected Vector getY() {
        return yVector;
    }

    /**
     * @return true if the model has no intercept term; false otherwise
     */
    public boolean isNoIntercept() {
        return noIntercept;
    }

    /**
     * @param noIntercept true means the model is to be estimated without an intercept term
     */
    public void setNoIntercept(boolean noIntercept) {
        this.noIntercept = noIntercept;
    }

    /**
     * <p>Loads model x and y sample data from a flat input array, overriding any previous sample.
     * </p>
     * <p>Assumes that rows are concatenated with y values first in each row.  For example, an input
     * <code>data</code> array containing the sequence of values (1, 2, 3, 4, 5, 6, 7, 8, 9) with
     * <code>nobs = 3</code> and <code>nvars = 2</code> creates a regression dataset with two
     * independent variables, as below:
     * <pre>
     *   y   x[0]  x[1]
     *   --------------
     *   1     2     3
     *   4     5     6
     *   7     8     9
     * </pre>
     * </p>
     * <p>Note that there is no need to add an initial unitary column (column of 1's) when
     * specifying a model including an intercept term.  If {@link #isNoIntercept()} is <code>true</code>,
     * the X matrix will be created without an initial column of "1"s; otherwise this column will
     * be added.
     * </p>
     * <p>Throws IllegalArgumentException if any of the following preconditions fail:
     * <ul><li><code>data</code> cannot be null</li>
     * <li><code>data.length = nobs * (nvars + 1)</li>
     * <li><code>nobs > nvars</code></li></ul>
     * </p>
     *
     * @param data input data array
     * @param nobs number of observations (rows)
     * @param nvars number of independent variables (columns, not counting y)
     * @param like matrix(maybe empty) indicating how data should be stored
     * @throws NullArgumentException if the data array is null
     * @throws CardinalityException if the length of the data array is not equal to <code>nobs * (nvars + 1)</code>
     * @throws InsufficientDataException if <code>nobs</code> is less than <code>nvars + 1</code>
     */
    public void newSampleData(double[] data, int nobs, int nvars, Matrix like) {
        if (data == null)
            throw new NullArgumentException();
        if (data.length != nobs * (nvars + 1))
            throw new CardinalityException(nobs * (nvars + 1), data.length);
        if (nobs <= nvars)
            throw new InsufficientDataException(RegressionsErrorMessages.INSUFFICIENT_OBSERVED_POINTS_IN_SAMPLE);
        double[] y = new double[nobs];
        final int cols = noIntercept ? nvars : nvars + 1;
        double[][] x = new double[nobs][cols];
        int pointer = 0;
        for (int i = 0; i < nobs; i++) {
            y[i] = data[pointer++];
            if (!noIntercept)
                x[i][0] = 1.0d;
            for (int j = noIntercept ? 0 : 1; j < cols; j++)
                x[i][j] = data[pointer++];
        }
        xMatrix = MatrixUtil.like(like, nobs, cols).assign(x);
        yVector = MatrixUtil.likeVector(like, y.length).assign(y);
    }

    /**
     * Loads new y sample data, overriding any previous data.
     *
     * @param y the array representing the y sample
     * @throws NullArgumentException if y is null
     * @throws NoDataException if y is empty
     */
    protected void newYSampleData(Vector y) {
        if (y == null)
            throw new NullArgumentException();
        if (y.size() == 0)
            throw new NoDataException();
        // TODO: IGNITE-5826, Should we copy here?
        yVector = y;
    }

    /**
     * <p>Loads new x sample data, overriding any previous data.
     * </p>
     * The input <code>x</code> array should have one row for each sample
     * observation, with columns corresponding to independent variables.
     * For example, if <pre>
     * <code> x = new double[][] {{1, 2}, {3, 4}, {5, 6}} </code></pre>
     * then <code>setXSampleData(x) </code> results in a model with two independent
     * variables and 3 observations:
     * <pre>
     *   x[0]  x[1]
     *   ----------
     *     1    2
     *     3    4
     *     5    6
     * </pre>
     * </p>
     * <p>Note that there is no need to add an initial unitary column (column of 1's) when
     * specifying a model including an intercept term.
     * </p>
     *
     * @param x the rectangular array representing the x sample
     * @throws NullArgumentException if x is null
     * @throws NoDataException if x is empty
     * @throws CardinalityException if x is not rectangular
     */
    protected void newXSampleData(Matrix x) {
        if (x == null)
            throw new NullArgumentException();
        if (x.rowSize() == 0)
            throw new NoDataException();
        if (noIntercept)
            // TODO: IGNITE-5826, Should we copy here?
            xMatrix = x;
        else { // Augment design matrix with initial unitary column
            xMatrix = MatrixUtil.like(x, x.rowSize(), x.columnSize() + 1);
            xMatrix.viewColumn(0).map(Functions.constant(1.0));
            xMatrix.viewPart(0, x.rowSize(), 1, x.columnSize()).assign(x);
        }
    }

    /**
     * Validates sample data.  Checks that
     * <ul><li>Neither x nor y is null or empty;</li>
     * <li>The length (i.e. number of rows) of x equals the length of y</li>
     * <li>x has at least one more row than it has columns (i.e. there is
     * sufficient data to estimate regression coefficients for each of the
     * columns in x plus an intercept.</li>
     * </ul>
     *
     * @param x the n x k matrix representing the x data
     * @param y the n-sized vector representing the y data
     * @throws NullArgumentException if {@code x} or {@code y} is null
     * @throws CardinalityException if {@code x} and {@code y} do not have the same length
     * @throws NoDataException if {@code x} or {@code y} are zero-length
     * @throws MathIllegalArgumentException if the number of rows of {@code x} is not larger than the number of columns
     * + 1
     */
    protected void validateSampleData(Matrix x, Vector y) throws MathIllegalArgumentException {
        if ((x == null) || (y == null))
            throw new NullArgumentException();
        if (x.rowSize() != y.size())
            throw new CardinalityException(y.size(), x.rowSize());
        if (x.rowSize() == 0) {  // Must be no y data either
            throw new NoDataException();
        }
        if (x.columnSize() + 1 > x.rowSize()) {
            throw new MathIllegalArgumentException(
                RegressionsErrorMessages.NOT_ENOUGH_DATA_FOR_NUMBER_OF_PREDICTORS,
                x.rowSize(), x.columnSize());
        }
    }

    /**
     * Validates that the x data and covariance matrix have the same
     * number of rows and that the covariance matrix is square.
     *
     * @param x the [n,k] array representing the x sample
     * @param covariance the [n,n] array representing the covariance matrix
     * @throws CardinalityException if the number of rows in x is not equal to the number of rows in covariance
     * @throws NonSquareMatrixException if the covariance matrix is not square
     */
    protected void validateCovarianceData(double[][] x, double[][] covariance) {
        if (x.length != covariance.length)
            throw new CardinalityException(x.length, covariance.length);
        if (covariance.length > 0 && covariance.length != covariance[0].length)
            throw new NonSquareMatrixException(covariance.length, covariance[0].length);
    }

    /**
     * {@inheritDoc}
     */
    @Override public double[] estimateRegressionParameters() {
        Vector b = calculateBeta();
        return b.getStorage().data();
    }

    /**
     * {@inheritDoc}
     */
    @Override public double[] estimateResiduals() {
        Vector b = calculateBeta();
        Vector e = yVector.minus(xMatrix.times(b));
        return e.getStorage().data();
    }

    /**
     * {@inheritDoc}
     */
    @Override public Matrix estimateRegressionParametersVariance() {
        return calculateBetaVariance();
    }

    /**
     * {@inheritDoc}
     */
    @Override public double[] estimateRegressionParametersStandardErrors() {
        Matrix betaVariance = estimateRegressionParametersVariance();
        double sigma = calculateErrorVariance();
        int len = betaVariance.rowSize();
        double[] res = new double[len];
        for (int i = 0; i < len; i++)
            res[i] = Math.sqrt(sigma * betaVariance.getX(i, i));
        return res;
    }

    /**
     * {@inheritDoc}
     */
    @Override public double estimateRegressandVariance() {
        return calculateYVariance();
    }

    /**
     * Estimates the variance of the error.
     *
     * @return estimate of the error variance
     */
    public double estimateErrorVariance() {
        return calculateErrorVariance();

    }

    /**
     * Estimates the standard error of the regression.
     *
     * @return regression standard error
     */
    public double estimateRegressionStandardError() {
        return Math.sqrt(estimateErrorVariance());
    }

    /**
     * Calculates the beta of multiple linear regression in matrix notation.
     *
     * @return beta
     */
    protected abstract Vector calculateBeta();

    /**
     * Calculates the beta variance of multiple linear regression in matrix
     * notation.
     *
     * @return beta variance
     */
    protected abstract Matrix calculateBetaVariance();

    /**
     * Calculates the variance of the y values.
     *
     * @return Y variance
     */
    protected double calculateYVariance() {
        // Compute initial estimate using definitional formula
        int vSize = yVector.size();
        double xbar = yVector.sum() / vSize;
        // Compute correction factor in second pass
        final double corr = yVector.foldMap((val, acc) -> acc + val - xbar, Functions.IDENTITY, 0.0);
        final double mean = xbar - corr;
        return yVector.foldMap(Functions.PLUS, val -> (val - mean) * (val - mean), 0.0) / (vSize - 1);
    }

    /**
     * <p>Calculates the variance of the error term.</p>
     * Uses the formula <pre>
     * var(u) = u &middot; u / (n - k)
     * </pre>
     * where n and k are the row and column dimensions of the design
     * matrix X.
     *
     * @return error variance estimate
     */
    protected double calculateErrorVariance() {
        Vector residuals = calculateResiduals();
        return residuals.dot(residuals) /
            (xMatrix.rowSize() - xMatrix.columnSize());
    }

    /**
     * Calculates the residuals of multiple linear regression in matrix
     * notation.
     *
     * <pre>
     * u = y - X * b
     * </pre>
     *
     * @return The residuals [n,1] matrix
     */
    protected Vector calculateResiduals() {
        Vector b = calculateBeta();
        return yVector.minus(xMatrix.times(b));
    }

}
