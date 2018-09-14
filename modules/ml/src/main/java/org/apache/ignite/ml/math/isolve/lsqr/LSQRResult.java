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

import java.util.Arrays;
import org.apache.ignite.ml.math.isolve.IterativeSolverResult;

/**
 * LSQR iterative solver result.
 */
public class LSQRResult extends IterativeSolverResult {
    /** */
    private static final long serialVersionUID = -8866269808589635947L;

    /**
     * Gives the reason for termination. 1 means x is an approximate solution to Ax = b. 2 means x approximately solves
     * the least-squares problem.
     */
    private final int isstop;

    /** Represents norm(r), where r = b - Ax. */
    private final double r1norm;

    /**Represents sqrt( norm(r)^2  +  damp^2 * norm(x)^2 ). Equal to r1norm if damp == 0. */
    private final double r2norm;

    /** Estimate of Frobenius norm of Abar = [[A]; [damp*I]]. */
    private final double anorm;

    /** Estimate of cond(Abar). */
    private final double acond;

    /** Estimate of norm(A'*r - damp^2*x). */
    private final double arnorm;

    /** Represents norm(x). */
    private final double xnorm;

    /**
     * If calc_var is True, estimates all diagonals of (A'A)^{-1} (if damp == 0) or more generally
     * (A'A + damp^2*I)^{-1}. This is well defined if A has full column rank or damp > 0.
     */
    private final double[] var;

    /**
     * Constructs a new instance of LSQR result.
     *
     * @param x X value.
     * @param iterations Number of performed iterations.
     * @param isstop Stop reason.
     * @param r1norm R1 norm value.
     * @param r2norm R2 norm value.
     * @param anorm A norm value.
     * @param acond A cond value.
     * @param arnorm AR norm value.
     * @param xnorm X norm value.
     * @param var Var value.
     */
    public LSQRResult(double[] x, int iterations, int isstop, double r1norm, double r2norm, double anorm, double acond,
        double arnorm, double xnorm, double[] var) {
        super(x, iterations);
        this.isstop = isstop;
        this.r1norm = r1norm;
        this.r2norm = r2norm;
        this.anorm = anorm;
        this.acond = acond;
        this.arnorm = arnorm;
        this.xnorm = xnorm;
        this.var = var;
    }

    /** */
    public int getIsstop() {
        return isstop;
    }

    /** */
    public double getR1norm() {
        return r1norm;
    }

    /** */
    public double getR2norm() {
        return r2norm;
    }

    /** */
    public double getAnorm() {
        return anorm;
    }

    /** */
    public double getAcond() {
        return acond;
    }

    /** */
    public double getArnorm() {
        return arnorm;
    }

    /** */
    public double getXnorm() {
        return xnorm;
    }

    /** */
    public double[] getVar() {
        return var;
    }

    /** */
    @Override public String toString() {
        return "LSQRResult{" +
            "isstop=" + isstop +
            ", r1norm=" + r1norm +
            ", r2norm=" + r2norm +
            ", anorm=" + anorm +
            ", acond=" + acond +
            ", arnorm=" + arnorm +
            ", xnorm=" + xnorm +
            ", var=" + Arrays.toString(var) +
            '}';
    }
}
