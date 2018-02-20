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

package org.apache.ignite.ml.math.isolve.lsqr;

import com.github.fommil.netlib.BLAS;
import java.util.Arrays;

/**
 * Basic implementation of the LSQR algorithm without assumptions about dataset storage format or data processing device.
 * LSQR finds a solution x to the following problems:
 * Unsymmetric equations:   solve       Ax = b
 * Linear least squares:    minimize    ||Ax - b||^2
 * Damped least squares:    minimize    ||Ax - b||^2 + d^2 * ||x||^2
 * Where A is a matrix with m by n dimensions and b is a vector with size m
 * The matrix A is normally be large and sparse. Only nonzero parts will be stored.
 * However, we note that in sparse least squares applications, A may have many more rows than columns (m >> n). In such cases it is vital to store A by rows.
 */
// TODO: IGNITE-7660: Refactor LSQR algorithm
public abstract class AbstractLSQR {
    /** The smallest representable positive number such that 1.0 + EPS != 1.0. */
    private static final double EPS = Double.longBitsToDouble(Double.doubleToLongBits(1.0) | 1) - 1.0;

    /** BLAS (Basic Linear Algebra Subprograms) instance. */
    private static BLAS blas = BLAS.getInstance();

    /**
     * Solves given Sparse Linear Systems.
     *
     * @param dampingCoefficient Damping coefficient, may be used to regularize ill-conditioned systems.
     * @param atol Stopping tolerances for an estimate of the relative error in the data definining the matrix A.
     * If both (atol and btol) are 1.0e-9 (say), the final residual norm should be accurate to about 9 digits.
     * @param btol Stopping tolerances for an estimate of the relative error in the data definining the Right hand-side vector b.
     * If both (atol and btol) are 1.0e-9 (say), the final residual norm should be accurate to about 9 digits.
     * @param conditionLimit Another stopping tolerance, LSQR terminates if an estimate of cond(A) exceeds conditionLimit, another input to 
     * regularize ill-conditioned systems.
     * @param iterationLimit Explicit limitation on number of iterations (for safety).
     * @param calculateVariable Whether to estimate diagonals of (A'A + damp^2*I)^{-1}.
     * @param x0 Initial value of x.
     * @return Solver result.
     */
    public LSQRResult solve(double dampingCoefficient, double atol, double btol, double conditionLimit, double iterationLimit, boolean calculateVariable,
        double[] x0) {
        // Init 'bookkeeping' variables
        int iterationCount = 0;
        int iterationStopCondition = 0;
        int numberOfColumns = getColumns();

        // When the iterationLimit has been given inappropriate value such as less than 0, it will get new value.
        if (iterationLimit < 0.0) {
            iterationLimit = 2.0 * numberOfColumns;
        }

        double[] var = new double[numberOfColumns];
        double ctol = 0.0;

        // Default iterationLimit
        if (iterationLimit < 0.0) {
            iterationLimit = 2.0 * numberOfColumns;
        }

        // Set ctol if a conditionLimit is entered
        if (conditionLimit > 0.0) {
            ctol = 1.0 / conditionLimit;
        }

        // Init norms
        double anorm = 0.0;
        double acond = 0.0;
        double ddnorm = 0.0;
        double res2 = 0.0;
        double xnorm = 0.0;
        double xxnorm = 0.0;
        double rnorm = 0.0;
        double r1norm = 0.0;
        double r2norm = 0.0;
        double arnorm = 0.0;

        // Set up the first vectors u and v for the bidiagonalization.
        // These satisfy  beta*u = b - A*x,  alfa*v = A'*u.
        double[][] initBidiag = initBidiagonalization(x0, numberOfColumns);
        double alfa = initBidiag[0][0];
        double beta = initBidiag[0][1];
        double bnorm = initBidiag[0][2];
        double[] x = initBidiag[1];
        double[] v = initBidiag[2];
        double[] w = initBidiag[3];

        // More variables used for bidiagonalization
        double rhobar = alfa;
        double phibar = beta;

        // Estimates of norms
        rnorm = beta;
        r1norm = rnorm;
        r2norm = rnorm;
        arnorm = alfa * beta;

        // Exit if arnorm indicates convergence
        if(isZero(arnorm)) {
            return new LSQRResult(x, iterationCount, iterationStopCondition, r1norm, r2norm, anorm, acond, arnorm, xnorm, var);
        }

        // Init vars for use in the loop
        //double res2 = 0;
        double z = 0.0;
        double cs2 = -1.0;
        double sn2 = 0.0;
        double[] dk = new double[w.length];

        // Variables used for plane rotations
        double cs, sn, t1, t2, rhs, psi, phi, theta, rho, tau, delta, gamma, rhobar1, gambar, zbar;

        // Other variables
        double[] iterBidiag, symOrtho;
        double res1, r1sq, test1, test2, test3, rtol;


        // Main iteration loop.
        while (iterationCount < iterationLimit) {
            iterationCount++; // We increment iteration counter here.

            // Perform the next step of the bidiagonalization to obtain the
            // next  beta, u, alfa, v.  These satisfy the relations
            // beta*u  =  A*v   -  alfa*u,
            // alfa*v  =  A'*u  -  beta*v.
            iterBidiag = iterBidiagonalization(alfa, dampingCoefficient, anorm, v);
            alfa = iterBidiag[0];
            beta = iterBidiag[1];
            anorm = iterBidiag[2];

            // Use a plane rotation to eliminate the damping parameter.
            // This alters the diagonal (rhobar) of the lower-bidiagonal matrix.
            rhobar1 = Math.sqrt(Math.pow(rhobar, 2) + Math.pow(dampingCoefficient, 2));
            cs = rhobar / rhobar1;
            sn = dampingCoefficient / rhobar1;
            psi = sn * phibar;
            phibar = cs * phibar;

            // Use a plane rotation to eliminate the subdiagonal element (beta)
            // of the lower-bidiagonal matrix, giving an upper-bidiagonal matrix.
            symOrtho = symOrtho(rhobar1, beta);
            cs = symOrtho[0];
            sn = symOrtho[1];
            rho = symOrtho[2];

            theta = sn * alfa;
            rhobar = -cs * alfa;
            phi = cs * phibar;
            phibar = sn * phibar;
            tau = sn * phi;

            t1 = phi / rho;
            t2 = -theta / rho;

            // Use a plane rotation on the right to eliminate the
            // super-diagonal element (theta) of the upper-bidiagonal matrix.
            // Then use the result to estimate norm(x).
            delta = sn2 * rho;
            gambar = -cs2 * rho;
            rhs = phi - delta * z;
            zbar = rhs / gambar;
            xnorm = Math.sqrt(xxnorm + Math.pow(zbar, 2));
            gamma = Math.sqrt(Math.pow(gambar, 2) + Math.pow(theta, 2));
            cs2 = gambar / gamma;
            sn2 = theta / gamma;
            z = rhs / gamma;
            xxnorm = xxnorm + Math.pow(z, 2);

            // Update x, w
            blas.dcopy(w.length, w, 1, dk, 1);
            blas.dscal(dk.length, 1 / rho, dk, 1);

            // x = x + t1*w
            blas.daxpy(w.length, t1, w, 1, x, 1);
            // w = v + t2*w
            blas.dscal(w.length, t2, w, 1);
            blas.daxpy(w.length, 1, v, 1, w, 1);

            ddnorm = ddnorm + Math.pow(blas.dnrm2(dk.length, dk, 1), 2);

            if (calculateVariable)
                blas.daxpy(var.length, 1.0, pow2(dk), 1, var, 1);

            // Test for convergence.
            // First, estimate the condition of the matrix  Abar,
            // and the norms of  rbar  and  Abar'rbar.
            acond = anorm * Math.sqrt(ddnorm);
            res1 = Math.pow(phibar, 2);
            res2 = res2 + Math.pow(psi, 2);
            rnorm = Math.sqrt(res1 + res2);
            arnorm = alfa * Math.abs(tau);

            // Distinguish between
            //    r1norm = ||b - Ax|| and
            //    r2norm = rnorm in current code
            //           = sqrt(r1norm^2 + damp^2*||x||^2).
            //    Estimate r1norm from
            //    r1norm = sqrt(r2norm^2 - damp^2*||x||^2).
            // Although there is cancellation, it might be accurate enough.
            r1sq = Math.pow(rnorm, 2) - Math.pow(dampingCoefficient, 2.0) * xxnorm;
            r1norm = Math.sqrt(Math.abs(r1sq));

            if (r1sq < 0) {
                r1norm *= -1;
            }

            r2norm = rnorm;

            // Now use these norms to estimate certain other quantities,
            // some of which will be small near a solution.
            test1 = rnorm / bnorm;
            test2 = arnorm / (anorm * rnorm + EPS);
            test3 = 1.0 / (acond + EPS);
            t1 = test1 / (1.0 + anorm * xnorm / bnorm);
            rtol = btol + atol * anorm * xnorm / bnorm;
            
            iterationStopCondition = nextIStop(test1, test2,  test3, iterationCount, t1, ctol, rtol, iterationLimit, atol);

            // If true, that means calculation terminated because of error
            if (iterationStopCondition != 0) {
                break;
            }
        }

        return new LSQRResult(x, iterationCount, iterationStopCondition, r1norm, r2norm, anorm, acond, arnorm, xnorm, var);
    }


    /**
     * Calculates the value of iStop for the next loop
     * The following tests guard against extremely small values of
     * atol, btol  or  ctol.  (The user may have set any or all of
     * the parameters  atol, btol, conditionLimit  to 0.)
     * The effect is equivalent to the normal tests using
     * atol = EPS,  btol = EPS, conditionLimit = 1/EPS.
     * @return iStop, condition representing current situation of algorithm.
     */
    private int nextIStop( double test1,  double test2,  double test3, int iterationCount, double t1, double ctol, double rtol, double iterationLimit, double atol) {
        int iStop=0;
        if (iterationCount >= iterationLimit) iStop = 7;
        if (1 + test3 <= 1) iStop = 6;
        if (1 + test2 <= 1) iStop = 5;
        if (1 + t1 <= 1) iStop = 4;

        // Allow for tolerances set by the user.
        if (test3 <= ctol) iStop = 3;
        if (test2 <= atol) iStop = 2;
        if (test1 <= rtol) iStop = 1;
        return iStop;
    }

    /**
     * Set up the first vectors u and v for the bidiagonalization.
     * These satisfy  beta*u = b - A*x,  alfa*v = A'*u.
     *
     * @param x0 Initial value of x.
     * @param numberOfColumns Number of columns in dataset.
     * @return double[][] {{alfa, beta, bnorm}, x, v, w}
     */
    private double[][] initBidiagonalization(double[] x0, int numberOfColumns) {
        // Scalars
        double bnorm = bnorm();
        double alfa;
        double beta;

        // Vectors
        double[] x;
        double[] v = new double[numberOfColumns];
        double[] w;

        // Setup beta and x
        if (x0 == null) {
            x = new double[numberOfColumns];
            beta = bnorm;
        } else {
            x = x0;
            beta = beta(x0, -1.0, 1.0);
        }

        // Setup alfa and v
        if (beta > 0) {
            v = iter(beta, v);
            alfa = blas.dnrm2(v.length, v, 1);
        } else {
            System.arraycopy(x, 0, v, 0, v.length);
            alfa = 0;
        }

        // Normalize v
        if (alfa > 0) {
            blas.dscal(v.length, 1 / alfa, v, 1);
        }

        // Make a copy of v
        w = Arrays.copyOf(v, v.length);

        return new double[][] {new double[]{alfa, beta, bnorm}, x, v, w};
    }

    /**
     * Perform the next step of the bidiagonalization to obtain the
     * next  beta, u, alfa, v.  These satisfy the relations
     * beta*u  =  A*v   -  alfa*u,
     * alfa*v  =  A'*u  -  beta*v.
     *
     * @param alfa Norm of v
     * @param dampingCoefficient Damping coefficient.
     * @param anorm Norm of A
     * @param vector Vector v
     * @return double[] {alfa, beta, anorm};
     */
    private double[] iterBidiagonalization(double alfa, double dampingCoefficient, double anorm, double[] vector) {
        double beta = beta(vector, 1.0, -alfa);
        if (beta > 0) {
            anorm = Math.sqrt(Math.pow(anorm, 2) + Math.pow(alfa, 2) + Math.pow(beta, 2) + Math.pow(dampingCoefficient, 2));

            blas.dscal(vector.length, -beta, vector, 1);

            iter(beta, vector);

            //vector = dataset.iter(beta, n);
            alfa = blas.dnrm2(vector.length, vector, 1);

            if (alfa > 0) {
                blas.dscal(vector.length, 1 / alfa, vector, 1);
            }
        }

        return new double[] {alfa, beta, anorm};
    }

    /**
     * Calculates bnorm.
     *
     * @return bnorm
     */
    protected abstract double bnorm();

    /**
     * Calculates beta.
     *
     * @param x X value.
     * @param alfa Alfa value.
     * @param beta Beta value.
     * @return Beta.
     */
    protected abstract double beta(double[] x, double alfa, double beta);

    /**
     * Perform LSQR iteration.
     *
     * @param bnorm Bnorm value.
     * @param target Target value.
     * @return Iteration result.
     */
    protected abstract double[] iter(double bnorm, double[] target);

    /** */
    protected abstract int getColumns();

    /** */
    private static double[] symOrtho(double a, double b) {
        if (isZero(b)) {
            return new double[] {Math.signum(a), 0, Math.abs(a)};
        } else if (isZero(a)) {
            return new double[] {0, Math.signum(b), Math.abs(b)};
        } else {
            double c, s, r;

            if (Math.abs(b) > Math.abs(a)) {
                double tau = a / b;
                s = Math.signum(b) / Math.sqrt(1 + tau * tau);
                c = s * tau;
                r = b / s;
            } else {
                double tau = b / a;
                c = Math.signum(a) / Math.sqrt(1 + tau * tau);
                s = c * tau;
                r = a / c;
            }
            return new double[] {c, s, r};
        }
    }

    /**
     * Raises all elements of the specified vector {@code a} to the power of the specified {@code pow}. Be aware that
     * it's "in place" operation.
     *
     * @param a Vector or matrix of doubles.
     * @return Matrix with elements raised to the specified power.
     */
    private static double[] pow2(double[] a) {
        double[] res = new double[a.length];

        for (int i = 0; i < res.length; i++)
            res[i] = Math.pow(a[i], 2);

        return res;
    }

    /*
    * Checks if double value is nummerically equal to zero.
    * @return true or false
    */
    private static boolean isZero(double value) {
        if(Double.compare(value, 0.0d) == 0) {
            return true;
        } else {
            return false;
        }
    }
}