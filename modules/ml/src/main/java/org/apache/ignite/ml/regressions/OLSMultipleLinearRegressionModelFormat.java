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

import java.io.Serializable;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.decompositions.QRDSolver;

/**
 * Linear regression model representation.
 *
 * @see OLSMultipleLinearRegressionModel
 */
public class OLSMultipleLinearRegressionModelFormat implements Serializable {
    /** X sample data. */
    private final Matrix xMatrix;

    /** Whether or not the regression model includes an intercept.  True means no intercept. */
    private final QRDSolver solver;

    /** */
    public OLSMultipleLinearRegressionModelFormat(Matrix xMatrix, QRDSolver solver) {
        this.xMatrix = xMatrix;
        this.solver = solver;
    }

    /** */
    public OLSMultipleLinearRegressionModel getOLSMultipleLinearRegressionModel() {
        return new OLSMultipleLinearRegressionModel(xMatrix, solver);
    }
}
