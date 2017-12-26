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

import org.apache.ignite.ml.Exportable;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.decompositions.QRDSolver;
import org.apache.ignite.ml.math.decompositions.QRDecomposition;

/**
 * Model for linear regression.
 */
public class OLSMultipleLinearRegressionModel implements Model<Vector, Vector>,
    Exportable<OLSMultipleLinearRegressionModelFormat> {
    /** */
    private final Matrix xMatrix;
    /** */
    private final QRDSolver solver;

    /**
     * Construct linear regression model.
     *
     * @param xMatrix See {@link QRDecomposition#QRDecomposition(Matrix)}.
     * @param solver Linear regression solver object.
     */
    public OLSMultipleLinearRegressionModel(Matrix xMatrix, QRDSolver solver) {
        this.xMatrix = xMatrix;
        this.solver = solver;
    }

    /** {@inheritDoc} */
    @Override public Vector apply(Vector val) {
        return xMatrix.times(solver.solve(val));
    }

    /** {@inheritDoc} */
    @Override public <P> void saveModel(Exporter<OLSMultipleLinearRegressionModelFormat, P> exporter, P path) {
        exporter.save(new OLSMultipleLinearRegressionModelFormat(xMatrix, solver), path);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        OLSMultipleLinearRegressionModel mdl = (OLSMultipleLinearRegressionModel)o;

        return xMatrix.equals(mdl.xMatrix) && solver.equals(mdl.solver);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = xMatrix.hashCode();
        res = 31 * res + solver.hashCode();
        return res;
    }
}
