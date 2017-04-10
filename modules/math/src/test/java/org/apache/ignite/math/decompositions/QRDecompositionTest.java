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

package org.apache.ignite.math.decompositions;

import org.apache.ignite.math.Matrix;
import org.apache.ignite.math.Tracer;
import org.apache.ignite.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.junit.Test;

import static org.junit.Assert.*;

/** */
public class QRDecompositionTest {
    /** */
    @Test
    public void basicTest() {
        DenseLocalOnHeapMatrix m = new DenseLocalOnHeapMatrix(new double[][]{
            {2.0d,  -1.0d,  0.0d},
            {-1.0d, 2.0d,  -1.0d},
            {0.0d, -1.0d, 2.0d}
        });

        QRDecomposition dec = new QRDecomposition(m);
        assertTrue("Unexpected value for full rank in decomposition " + dec, dec.hasFullRank());

        Matrix q = dec.getQ();
        Matrix r = dec.getR();

        assertNotNull("Matrix l is expected to be not null.", q);
        assertNotNull("Matrix lt is expected to be not null.", r);

        Matrix expIdentity = q.times(q.transpose());

        final double delta = 0.0001;

        for (int row = 0; row < expIdentity.rowSize(); row++)
            for (int col = 0; col < expIdentity.columnSize(); col++)
                assertEquals("Unexpected identity matrix value at (" + row + "," + col + ").",
                    row == col ? 1d : 0d, expIdentity.get(col, row), delta);

        for (int row = 0; row < r.rowSize(); row++)
            for (int col = 0; col < row - 1; col++)
                assertEquals("Unexpected upper triangular matrix value at (" + row + "," + col + ").",
                    0d, r.get(row, col), delta);

        Matrix recomposed = q.times(r);

        for (int row = 0; row < m.rowSize(); row++)
            for (int col = 0; col < m.columnSize(); col++)
                assertEquals("Unexpected recomposed matrix value at (" + row + "," + col + ").",
                    m.get(row, col), recomposed.get(row, col), delta);

        Matrix sol = dec.solve(new DenseLocalOnHeapMatrix(3, 10));
        assertEquals("Unexpected rows in solution matrix.", 3, sol.rowSize());
        assertEquals("Unexpected cols in solution matrix.", 10, sol.columnSize());

        Tracer.showAscii(sol);
        for (int row = 0; row < sol.rowSize(); row++)
            for (int col = 0; col < sol.columnSize(); col++)
                assertEquals("Unexpected solution matrix value at (" + row + "," + col + ").",
                    0d, sol.get(row, col), delta);

        dec.destroy();

        QRDecomposition dec1 = new QRDecomposition(new DenseLocalOnHeapMatrix(new double[][]{
            {2.0d,  -1.0d},
            {-1.0d, 2.0d},
            {0.0d, -1.0d}
        }));

        assertTrue("Unexpected value for full rank in decomposition " + dec1, dec1.hasFullRank());

        dec1.destroy();

        QRDecomposition dec2 = new QRDecomposition(new DenseLocalOnHeapMatrix(new double[][]{
            {2.0d,  -1.0d,  0.0d, 0.0d},
            {-1.0d, 2.0d,  -1.0d, 0.0d},
            {0.0d, -1.0d, 2.0d, 0.0d}
        }));

        assertTrue("Unexpected value for full rank in decomposition " + dec2, dec2.hasFullRank());

        dec2.destroy();
    }

    /** */
    @Test(expected = IllegalArgumentException.class)
    public void solveWrongMatrixSizeTest() {
        new QRDecomposition(new DenseLocalOnHeapMatrix(new double[][]{
            {2.0d,  -1.0d,  0.0d},
            {-1.0d, 2.0d,  -1.0d},
            {0.0d, -1.0d, 2.0d}
        })).solve(new DenseLocalOnHeapMatrix(2, 3));
    }
}
