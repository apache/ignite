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

package org.apache.ignite.ml.math.decompositions;

import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.math.impls.matrix.PivotedMatrixView;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** */
public class SingularValueDecompositionTest {
    /** */
    @Test
    public void basicTest() {
        basicTest(new DenseLocalOnHeapMatrix(new double[][] {
            {2.0d, -1.0d, 0.0d},
            {-1.0d, 2.0d, -1.0d},
            {0.0d, -1.0d, 2.0d}
        }));
    }

    /**
     * Test for {@link DecompositionSupport} features.
     */
    @Test
    public void decompositionSupportTest() {
        basicTest(new PivotedMatrixView(new DenseLocalOnHeapMatrix(new double[][] {
            {2.0d, -1.0d, 0.0d},
            {-1.0d, 2.0d, -1.0d},
            {0.0d, -1.0d, 2.0d}
        })));
    }

    /** */
    @Test
    public void rowsLessThanColumnsTest() {
        DenseLocalOnHeapMatrix m = new DenseLocalOnHeapMatrix(new double[][] {
            {2.0d, -1.0d, 0.0d},
            {-1.0d, 2.0d, -1.0d}
        });

        SingularValueDecomposition dec = new SingularValueDecomposition(m);
        assertEquals("Unexpected value for singular values size.",
            2, dec.getSingularValues().length);

        Matrix s = dec.getS();
        Matrix u = dec.getU();
        Matrix v = dec.getV();
        Matrix covariance = dec.getCovariance(0.5);

        assertNotNull("Matrix s is expected to be not null.", s);
        assertNotNull("Matrix u is expected to be not null.", u);
        assertNotNull("Matrix v is expected to be not null.", v);
        assertNotNull("Covariance matrix is expected to be not null.", covariance);

        dec.destroy();
    }

    /** */
    @Test(expected = AssertionError.class)
    public void nullMatrixTest() {
        new SingularValueDecomposition(null);
    }

    /** */
    private void basicTest(Matrix m) {
        SingularValueDecomposition dec = new SingularValueDecomposition(m);
        assertEquals("Unexpected value for singular values size.",
            3, dec.getSingularValues().length);

        Matrix s = dec.getS();
        Matrix u = dec.getU();
        Matrix v = dec.getV();
        Matrix covariance = dec.getCovariance(0.5);

        assertNotNull("Matrix s is expected to be not null.", s);
        assertNotNull("Matrix u is expected to be not null.", u);
        assertNotNull("Matrix v is expected to be not null.", v);
        assertNotNull("Covariance matrix is expected to be not null.", covariance);

        assertTrue("Decomposition cond is expected to be positive.", dec.cond() > 0);
        assertTrue("Decomposition norm2 is expected to be positive.", dec.norm2() > 0);
        assertEquals("Decomposition rank differs from expected.", 3, dec.rank());
        assertEquals("Decomposition singular values size differs from expected.",
            3, dec.getSingularValues().length);

        Matrix recomposed = (u.times(s).times(v.transpose()));

        for (int row = 0; row < m.rowSize(); row++)
            for (int col = 0; col < m.columnSize(); col++)
                assertEquals("Unexpected recomposed matrix value at (" + row + "," + col + ").",
                    m.get(row, col), recomposed.get(row, col), 0.001);

        for (int row = 0; row < covariance.rowSize(); row++)
            for (int col = row + 1; col < covariance.columnSize(); col++)
                assertEquals("Unexpected covariance matrix value at (" + row + "," + col + ").",
                    covariance.get(row, col), covariance.get(col, row), 0.001);

        dec.destroy();
    }
}
