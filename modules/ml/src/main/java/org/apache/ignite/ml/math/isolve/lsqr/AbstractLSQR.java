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
 * Basic implementation of the LSQR algorithm without assumptions about dataset storage format or data processing
 * device.
 *
 * This implementation is based on SciPy implementation.
 * SciPy implementation: https://github.com/scipy/scipy/blob/master/scipy/sparse/linalg/isolve/lsqr.py#L98.
 */
// TODO: IGNITE-7660: Refactor LSQR algorithm
public abstract class AbstractLSQR {
    /**
     * <p>
     * Largest double-precision floating-point number such that
     * {@code 1 + EPSILON} is numerically equal to 1. This value is an upper
     * bound on the relative error due to rounding real numbers to double
     * precision floating-point numbers.
     * </p>
     * <p>
     * In IEEE 754 arithmetic, this is 2<sup>-53</sup>.
     * </p>
     *
     * @see <a href="http://en.wikipedia.org/wiki/Machine_epsilon">Machine epsilon</a>
     */
    private static final double EPSILON;

    /** Exponent offset in IEEE754 representation. */
    private static final long EXPONENT_OFFSET = 1023L;

    static {
        /*
         *  This was previously expressed as = 0x1.0p-53;
         *  However, OpenJDK (Sparc Solaris) cannot handle such small
         *  constants: MATH-721
         */
        EPSILON = Double.longBitsToDouble((EXPONENT_OFFSET - 53L) << 52);
    }

    /** The smallest representable positive number such that 1.0 + eps != 1.0. */
    private static final double eps = EPSILON;

    /** BLAS (Basic Linear Algebra Subprograms) instance. */
    private static BLAS blas = BLAS.getInstance();

    /**
     * Solves given Sparse Linear Systems.
     *
     * @param damp Damping coefficient.
     * @param atol Stopping tolerances, if both (atol and btol) are 1.0e-9 (say), the final residual norm should be
     * accurate to about 9 digits.
     * @param btol Stopping tolerances, if both (atol and btol) are 1.0e-9 (say), the final residual norm should be
     * accurate to about 9 digits.
     * @param conlim Another stopping tolerance, LSQR terminates if an estimate of cond(A) exceeds conlim.
     * @param iterLim Explicit limitation on number of iterations (for safety).
     * @param calcVar Whether to estimate diagonals of (A'A + damp^2*I)^{-1}.
     * @param x0 Initial value of x.
     * @return Solver result.
     */
    public LSQRResult solve(double damp, double atol, double btol, double conlim, double iterLim, boolean calcVar,
        double[] x0) {
        int n = getColumns();

        if (iterLim < 0)
            iterLim = 2 * n;

        double[] var = new double[n];
        int itn = 0;
        int istop = 0;
        double ctol = 0;

        if (conlim > 0)
            ctol = 1 / conlim;

        double anorm = 0;
        double acond = 0;
        double dampsq = Math.pow(damp, 2.0);
        double ddnorm = 0;
        double res2 = 0;
        double xnorm = 0;
        double xxnorm = 0;
        double z = 0;
        double cs2 = -1;
        double sn2 = 0;

        // Set up the first vectors u and v for the bidiagonalization.
        // These satisfy  beta*u = b - A*x,  alfa*v = A'*u.
        double bnorm = bnorm();
        double[] x;
        double beta;

        if (x0 == null) {
            x = new double[n];
            beta = bnorm;
        }
        else {
            x = x0;
            beta = beta(x, -1.0, 1.0);
        }

        double[] v = new double[n];
        double alfa;

        if (beta > 0) {
            v = iter(beta, v);
            alfa = blas.dnrm2(v.length, v, 1);
        }
        else {
            System.arraycopy(x, 0, v, 0, v.length);
            alfa = 0;
        }

        if (alfa > 0)
            blas.dscal(v.length, 1 / alfa, v, 1);

        double[] w = Arrays.copyOf(v, v.length);

        double rhobar = alfa;
        double phibar = beta;
        double rnorm = beta;
        double r1norm = rnorm;
        double r2norm = rnorm;
        double arnorm = alfa * beta;
        double[] dk = new double[w.length];

        if (arnorm == 0)
            return new LSQRResult(x, itn, istop, r1norm, r2norm, anorm, acond, arnorm, xnorm, var);

        // Main iteration loop.
        while (itn < iterLim) {
            itn = itn + 1;

            // Perform the next step of the bidiagonalization to obtain the
            // next  beta, u, alfa, v.  These satisfy the relations
            //            beta*u  =  A*v   -  alfa*u,
            //            alfa*v  =  A'*u  -  beta*v.
            beta = beta(v, 1.0, -alfa);
            if (beta > 0) {
                anorm = Math.sqrt(Math.pow(anorm, 2) + Math.pow(alfa, 2) + Math.pow(beta, 2) + Math.pow(damp, 2));

                blas.dscal(v.length, -beta, v, 1);

                iter(beta, v);

                //v = dataset.iter(beta, n);
                alfa = blas.dnrm2(v.length, v, 1);

                if (alfa > 0)
                    blas.dscal(v.length, 1 / alfa, v, 1);
            }

            // Use a plane rotation to eliminate the damping parameter.
            // This alters the diagonal (rhobar) of the lower-bidiagonal matrix.
            double rhobar1 = Math.sqrt(Math.pow(rhobar, 2) + Math.pow(damp, 2));
            double cs1 = rhobar / rhobar1;
            double sn1 = damp / rhobar1;
            double psi = sn1 * phibar;
            phibar = cs1 * phibar;

            // Use a plane rotation to eliminate the subdiagonal element (beta)
            // of the lower-bidiagonal matrix, giving an upper-bidiagonal matrix.
            double[] symOrtho = symOrtho(rhobar1, beta);
            double cs = symOrtho[0];
            double sn = symOrtho[1];
            double rho = symOrtho[2];

            double theta = sn * alfa;
            rhobar = -cs * alfa;
            double phi = cs * phibar;
            phibar = sn * phibar;
            double tau = sn * phi;

            double t1 = phi / rho;
            double t2 = -theta / rho;

            blas.dcopy(w.length, w, 1, dk, 1);
            blas.dscal(dk.length, 1 / rho, dk, 1);

            // x = x + t1*w
            blas.daxpy(w.length, t1, w, 1, x, 1);
            // w = v + t2*w
            blas.dscal(w.length, t2, w, 1);
            blas.daxpy(w.length, 1, v, 1, w, 1);

            ddnorm = ddnorm + Math.pow(blas.dnrm2(dk.length, dk, 1), 2);

            if (calcVar)
                blas.daxpy(var.length, 1.0, pow2(dk), 1, var, 1);

            // Use a plane rotation on the right to eliminate the
            // super-diagonal element (theta) of the upper-bidiagonal matrix.
            // Then use the result to estimate norm(x).
            double delta = sn2 * rho;
            double gambar = -cs2 * rho;
            double rhs = phi - delta * z;
            double zbar = rhs / gambar;
            xnorm = Math.sqrt(xxnorm + Math.pow(zbar, 2));
            double gamma = Math.sqrt(Math.pow(gambar, 2) + Math.pow(theta, 2));
            cs2 = gambar / gamma;
            sn2 = theta / gamma;
            z = rhs / gamma;
            xxnorm = xxnorm + Math.pow(z, 2);

            // Test for convergence.
            // First, estimate the condition of the matrix  Abar,
            // and the norms of  rbar  and  Abar'rbar.
            acond = anorm * Math.sqrt(ddnorm);
            double res1 = Math.pow(phibar, 2);
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
            double r1sq = Math.pow(rnorm, 2) - dampsq * xxnorm;
            r1norm = Math.sqrt(Math.abs(r1sq));

            if (r1sq < 0)
                r1norm = -r1norm;

            r2norm = rnorm;

            // Now use these norms to estimate certain other quantities,
            // some of which will be small near a solution.
            double test1 = rnorm / bnorm;
            double test2 = arnorm / (anorm * rnorm + eps);
            double test3 = 1 / (acond + eps);
            t1 = test1 / (1 + anorm * xnorm / bnorm);
            double rtol = btol + atol * anorm * xnorm / bnorm;

            // The following tests guard against extremely small values of
            // atol, btol  or  ctol.  (The user may have set any or all of
            // the parameters  atol, btol, conlim  to 0.)
            // The effect is equivalent to the normal tests using
            // atol = eps,  btol = eps,  conlim = 1/eps.
            if (itn >= iterLim)
                istop = 7;

            if (1 + test3 <= 1)
                istop = 6;

            if (1 + test2 <= 1)
                istop = 5;

            if (1 + t1 <= 1)
                istop = 4;

            // Allow for tolerances set by the user.
            if (test3 <= ctol)
                istop = 3;

            if (test2 <= atol)
                istop = 2;

            if (test1 <= rtol)
                istop = 1;

            if (istop != 0)
                break;
        }

        return new LSQRResult(x, itn, istop, r1norm, r2norm, anorm, acond, arnorm, xnorm, var);
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
        if (b == 0)
            return new double[] {Math.signum(a), 0, Math.abs(a)};
        else if (a == 0)
            return new double[] {0, Math.signum(b), Math.abs(b)};
        else {
            double c, s, r;

            if (Math.abs(b) > Math.abs(a)) {
                double tau = a / b;
                s = Math.signum(b) / Math.sqrt(1 + tau * tau);
                c = s * tau;
                r = b / s;
            }
            else {
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

}
