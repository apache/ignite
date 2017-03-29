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
import org.apache.ignite.math.Vector;
import org.apache.ignite.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link LUDecomposition}.
 */
public class LUDecompositionTest {
    private Matrix testMatrix;
    private Matrix testL;
    private Matrix testU;
    private Matrix testP;

    private LUDecomposition luDecomposition;

    int[] rawPivot;

    /** */
    @Before
    public void setUp(){
        double[][] rawMatrix = new double[][] {
            {2.0d, 1.0d, 1.0d, 0.0d},
            {4.0d, 3.0d, 3.0d, 1.0d},
            {8.0d, 7.0d, 9.0d, 5.0d},
            {6.0d, 7.0d, 9.0d, 8.0d}};
        double[][] rawL = {
            {1.0d, 0.0d, 0.0d, 0.0d},
            {3.0d/4.0d, 1.0d, 0.0d, 0.0d},
            {1.0d/2.0d, -2.0d/7.0d, 1.0d, 0.0d},
            {1.0d/4.0d, -3.0d/7.0d, 1.0d/3.0d, 1.0d}};
        double[][] rawU = {
            {8.0d, 7.0d, 9.0d, 5.0d},
            {0.0d, 7.0d/4.0d, 9.0d/4.0d, 17.0d/4.0d},
            {0.0d, 0.0d, -6.0d/7.0d, -2.0d/7.0d},
            {0.0d, 0.0d, 0.0d, 2.0d/3.0d}};
        double[][] rawP = new double[][] {
            {0, 0, 1.0d, 0},
            {0, 0, 0, 1.0d},
            {0, 1.0d, 0, 0},
            {1.0d, 0, 0, 0}};

        rawPivot = new int[] {3, 4, 2, 1};

        testMatrix = new DenseLocalOnHeapMatrix(rawMatrix);
        testL = new DenseLocalOnHeapMatrix(rawL);
        testU = new DenseLocalOnHeapMatrix(rawU);
        testP = new DenseLocalOnHeapMatrix(rawP);

        luDecomposition = new LUDecomposition(testMatrix);
    }

    /** */
    @Test
    public void getL() throws Exception {
        Matrix luDecompositionL = luDecomposition.getL();

        assertEquals("Value should be equal.", testL.rowSize(), luDecompositionL.rowSize());
        assertEquals("Value should be equal.", testL.columnSize(), luDecompositionL.columnSize());

        for (int i = 0; i < testL.rowSize(); i++)
            for (int j = 0; j < testL.columnSize(); j++)
                assertEquals("Value should be equal.", testL.getX(i, j), luDecompositionL.getX(i, j), 0.0000001d);
    }

    /** */
    @Test
    public void getU() throws Exception {
        Matrix luDecompositionU = luDecomposition.getU();

        assertEquals("Value should be equal.", testU.rowSize(), luDecompositionU.rowSize());
        assertEquals("Value should be equal.", testU.columnSize(), luDecompositionU.columnSize());

        for (int i = 0; i < testU.rowSize(); i++)
            for (int j = 0; j < testU.columnSize(); j++)
                assertEquals("Value should be equal.", testU.getX(i, j), luDecompositionU.getX(i, j), 0.0000001d);
    }

    /** */
    @Test
    public void getP() throws Exception {
        Matrix luDecompositionP = luDecomposition.getP();

        assertEquals("Value should be equal.", testP.rowSize(), luDecompositionP.rowSize());
        assertEquals("Value should be equal.", testP.columnSize(), luDecompositionP.columnSize());

        for (int i = 0; i < testP.rowSize(); i++)
            for (int j = 0; j < testP.columnSize(); j++)
                assertEquals("Value should be equal.", testP.getX(i, j), luDecompositionP.getX(i, j), 0.0000001d);
    }

    /** */
    @Test
    public void getPivot() throws Exception {
        Vector pivot = luDecomposition.getPivot();

        assertEquals("Value should be equal.", rawPivot.length, pivot.size());

        for (int i = 0; i < testU.rowSize(); i++)
            assertEquals("Value should be equal.", (int)pivot.get(i) + 1, rawPivot[i]);
    }
}