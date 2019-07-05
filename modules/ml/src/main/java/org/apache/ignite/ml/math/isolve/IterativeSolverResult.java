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

package org.apache.ignite.ml.math.isolve;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Base class for iterative solver results.
 */
public class IterativeSolverResult implements Serializable {
    /** */
    private static final long serialVersionUID = 8084061028708491097L;

    /** The final solution. */
    private final double[] x;

    /** Iteration number upon termination. */
    private final int iterations;

    /**
     * Constructs a new instance of iterative solver result.
     *
     * @param x The final solution.
     * @param iterations Iteration number upon termination.
     */
    public IterativeSolverResult(double[] x, int iterations) {
        this.x = x;
        this.iterations = iterations;
    }

    /** */
    public double[] getX() {
        return x;
    }

    /** */
    public int getIterations() {
        return iterations;
    }

    /** */
    @Override public String toString() {
        return "IterativeSolverResult{" +
            "x=" + Arrays.toString(x) +
            ", iterations=" + iterations +
            '}';
    }
}
