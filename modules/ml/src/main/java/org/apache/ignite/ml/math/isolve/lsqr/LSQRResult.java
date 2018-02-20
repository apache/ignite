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
     * Gives the reason for termination. 
     * 0 means x is equal to 0 vector. No iteration were performed.
     * 1 means x is an approximate solution to Ax = b. 
     * 2 means x approximately solves
     * 3 means an estimate of condition of matrix Abar has exceeded conlim. The system Ax = b appears to be ill-conditioned.
     * 4 means the system Ax = b is probably campatible. Norm(Ax - b) is as small as seems reasonable on this machine.
     * 5 means the system Ax = b is probably not compatible. A least-squares solution has been obtained which is as accurate as seems reasonable on this machine.
     * 6 means condition of martix Abar seems to be so large such that there is not much point in doing further iterations.
     * 7 means the iteration limit "iterLimit" has been reached.
     * the least-squares problem.
     */
    private final int iterationStopCondition;

    /** Represents norm(r), which represents standart error estimation for the component of x, where r = b - Ax. */
    private final double r1Norm;

    /**
     * Represents sqrt( norm(r)^2  +  damp^2 * norm(x)^2 ), which is another representation of 
     * standart error estimation for the component of x. Equal to r1norm if damp == 0. 
     */
    private final double r2Norm;

    /** Estimate of Frobenius norm of Abar = [[A]; [damp*I]]. */
    private final double aNorm;

    /** Estimate of cond(Abar), the condition number of Abar. Very high value of acond may indicate error. */
    private final double aCondition;

    /** Estimate of norm(A'*r - damp^2*x), the norm of the residual for the usual normal equations.
     *  This should be small in all cases. 
     *  arnorm will often will be smaller than the true value computed from the output vector x.
     */
    private final double arNorm;

    /** Represents norm(x), an estimate of the norm of the final solution vector x. */
    private final double xNorm;

    /**
     * If calc_var is True, estimates all diagonals of (A'A)^{-1} (if damp == 0) or more generally
     * (A'A + damp^2*I)^{-1}. This is well defined if A has full column rank or damp > 0.
     */
    private final double[] estimatedDiagonalVector;

    /**
     * Constructs a new instance of LSQR result.
     *
     * @param x X value.
     * @param iterations Number of performed iterations.
     * @param iterationStopCondition Stop reason.
     * @param r1Norm R1 norm value.
     * @param r2Norm R2 norm value.
     * @param aNorm A norm value.
     * @param aCondition A cond value.
     * @param arNorm AR norm value.
     * @param xNorm X norm value.
     * @param estimatedDiagonalVector Var value.
     */
    public LSQRResult(double[] x, int iterations, int iterationStopCondition, double r1Norm, double r2Norm, double aNorm, double aCondition,
        double arNorm, double xNorm, double[] estimatedDiagonalVector) {
        super(x, iterations);
        this.iterationStopCondition = iterationStopCondition;
        this.r1Norm = r1Norm;
        this.r2Norm = r2Norm;
        this.aNorm = aNorm;
        this.aCondition = aCondition;
        this.arNorm = arNorm;
        this.xNorm = xNorm;
        this.estimatedDiagonalVector = estimatedDiagonalVector;
    }

    /** */
    public int getIterationStopCondition() {
        return iterationStopCondition;
    }

    /** */
    public double getR1Norm() {
        return r1Norm;
    }

    /** */
    public double getR2Norm() {
        return r2Norm;
    }

    /** */
    public double getANorm() {
        return aNorm;
    }

    /** */
    public double getACondition() {
        return aCondition;
    }

    /** */
    public double getArNorm() {
        return arNorm;
    }

    /** */
    public double getXNorm() {
        return xNorm;
    }

    /** */
    public double[] getEstimatedDiagonalVector() {
        return estimatedDiagonalVector;
    }

    /** */
    @Override public String toString() {
        return "LSQRResult{" +
            "iterationStopCondition=" + iterationStopCondition +
            ", r1Norm=" + r1Norm +
            ", r2Norm=" + r2Norm +
            ", aNorm=" + aNorm +
            ", aCondition=" + aCondition +
            ", arNorm=" + arNorm +
            ", xNorm=" + xNorm +
            ", estimatedDiagonalVector=" + Arrays.toString(estimatedDiagonalVector) +
            '}';
    }
}