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

package org.apache.ignite.math.impls.matrix;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.ignite.math.Matrix;
import org.apache.ignite.math.ExternalizeTest;
import org.apache.ignite.math.Vector;
import org.apache.ignite.math.exceptions.CardinalityException;
import org.apache.ignite.math.functions.IntIntToDoubleFunction;
import org.apache.ignite.math.impls.vector.DenseLocalOffHeapVector;
import org.apache.ignite.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.math.impls.vector.RandomVector;
import org.junit.Test;

import java.util.function.BiConsumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link Matrix} implementations.
 */
public class MatrixImplementationsTest extends ExternalizeTest<Matrix> {
    private static final double DEFAULT_DELTA = 0.000000001d;

    /** */
    private static final Map<Class<? extends Matrix>, Class<? extends Vector>> typesMap = typesMap();

    /** */
    private void consumeSampleMatrix(BiConsumer<Matrix, String> consumer) {
        new MatrixImplementationFixtures().consumeSampleMatrix(null, consumer);
    }

    /** */
    @Test
    public void externalizeTest(){
        consumeSampleMatrix((m, desc)-> externalizeTest(m));
    }

    /** */
    @Test
    public void likeTest(){
        consumeSampleMatrix((m, desc) -> {
            Matrix like = m.like(m.rowSize(), m.columnSize());

            assertEquals("Wrong \"like\" matrix for "+ desc + "; Unexpected class: " + like.getClass().toString(),
                    like.getClass(),
                    m.getClass());
            assertEquals("Wrong \"like\" matrix for "+ desc + "; Unexpected rows.", like.rowSize(), m.rowSize());
            assertEquals("Wrong \"like\" matrix for "+ desc + "; Unexpected columns.", like.columnSize(), m.columnSize());
            assertEquals("Wrong \"like\" matrix for "+ desc + "; Unexpected storage class: " + like.getStorage().getClass().toString(),
                    like.getStorage().getClass(),
                    m.getStorage().getClass());
        });
    }

    /** */
    @Test
    public void copyTest(){
        consumeSampleMatrix((m, desc) -> {
            Matrix cp = m.copy();
            assertTrue("Incorrect copy for empty matrix " + desc, cp.equals(m));

            if (ignore(m.getClass()))
                return;

            fillMatrix(m);
            cp = m.copy();

            assertTrue("Incorrect copy for matrix " + desc, cp.equals(m));
        });
    }

    /** */ @Test
    public void testHaveLikeVector() throws InstantiationException, IllegalAccessException {
        for (Class<? extends Matrix> key : typesMap.keySet()) {
            Class<? extends Vector> val = typesMap.get(key);

            if (val == null && !ignore(key))
                System.out.println("Missing test for implementation of likeMatrix for " + key.getSimpleName());
        }
    }

    /** */
    @Test
    public void testLikeVector(){
        consumeSampleMatrix((m, desc)->{
            if (typesMap().containsKey(m.getClass())){
                Vector likeVector = m.likeVector(m.columnSize());

                assertNotNull(likeVector);
                assertEquals("Unexpected value.", likeVector.size(), m.columnSize());
            }
        });
    }

    /** */
    @Test
    public void testAssignSingleElement(){
        consumeSampleMatrix((m,desc) -> {
            if (ignore(m.getClass()))
                return;

            final double assignVal = Math.random();

            m.assign(assignVal);

            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    assertTrue("Unexpected value.", Double.compare(m.get(i, j), assignVal) == 0);
        });
    }

    /** */
    @Test
    public void testAssignArray(){
        consumeSampleMatrix((m,desc) -> {
            if (ignore(m.getClass()))
                return;

            double[][] arr = new double[m.rowSize()][m.columnSize()];

            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    arr[i][j] = Math.random();

            m.assign(arr);

            for (int i = 0; i < m.rowSize(); i++) {
                for (int j = 0; j < m.columnSize(); j++)
                    assertTrue("Unexpected value.", Double.compare(m.get(i, j), arr[i][j]) == 0);
            }
        });
    }

    /** */
    @Test
    public void testPlus(){
        consumeSampleMatrix((m, desc)->{
            if (ignore(m.getClass()))
                return;

            fillMatrix(m);

            double plusVal = Math.random();

            Matrix plus = m.plus(plusVal);

            for(int i = 0; i < m.rowSize(); i++)
                for(int j = 0; j < m.columnSize(); j++)
                  assertTrue("Unexpected value.", Double.compare(plus.get(i, j), m.get(i, j) + plusVal) == 0);
        });
    }

    /** */
    @Test
    public void testTimes(){
        consumeSampleMatrix((m, desc)->{
            if (ignore(m.getClass()))
                return;

            fillMatrix(m);

            double timeVal = Math.random();
            Matrix times = m.times(timeVal);

            for(int i = 0; i < m.rowSize(); i++)
                for(int j = 0; j < m.columnSize(); j++)
                    assertTrue("Unexpected value.", Double.compare(times.get(i, j), m.get(i, j) * timeVal) == 0);
        });
    }

    /** */
    @Test
    public void testDivide(){
        consumeSampleMatrix((m, desc)->{
            if (ignore(m.getClass()))
                return;

            double[][] arr = fillAndReturn(m);

            double divVal = Math.random();

            Matrix divide = m.divide(divVal);

            for(int i = 0; i < m.rowSize(); i++)
                for(int j = 0; j < m.columnSize(); j++)
                    assertTrue("Unexpected value.", Double.compare(divide.get(i, j), arr[i][j] / divVal) == 0);
        });
    }

    /** */
    @Test
    public void testTranspose(){
        consumeSampleMatrix((m, desc)->{
            if (ignore(m.getClass()))
                return;

            fillMatrix(m);

            Matrix transpose = m.transpose();

            for(int i = 0; i < m.rowSize(); i++)
                for(int j = 0; j < m.columnSize(); j++)
                    assertTrue("Unexpected value.", Double.compare(m.get(i, j), transpose.get(j, i)) == 0);
        });
    }
    
    /** */
    @Test
    public void testDeterminant(){
        consumeSampleMatrix((m, desc)->{
            if (m.rowSize() != m.columnSize())
                return;

            double[][] doubles = fillIntAndReturn(m);

            if (m.rowSize() == 1)
                assertEquals("Unexpected value " + desc, m.determinant(), doubles[0][0], 0d);
            else if (m.rowSize() == 2){
                double det = doubles[0][0] * doubles[1][1] - doubles[0][1] * doubles[1][0];
                assertEquals("Unexpected value " + desc, m.determinant(), det, 0d);
            } else {
                if (ignore(m.getClass()))
                    return;

                if (m.rowSize() < 30000) { //Otherwise it's takes too long.
                    Matrix diagMtx = m.like(m.rowSize(), m.columnSize());

                    diagMtx.assign(0);
                    for (int i = 0; i < m.rowSize(); i++)
                        diagMtx.set(i, i, m.get(i, i));

                    double det = 1;

                    for (int i = 0; i < diagMtx.rowSize(); i++)
                        det *= diagMtx.get(i, i);

                    try {
                        assertEquals("Unexpected value " + desc, det, diagMtx.determinant(), DEFAULT_DELTA);
                    } catch (Exception e){
                        System.out.println(desc);
                        throw e;
                    }
                }
            }
        });
    }

    /** */
    @Test
    public void testMap(){
        consumeSampleMatrix((m, desc)->{
            if (ignore(m.getClass()))
                return;

            fillMatrix(m);

            m.map(x -> 0d);

            for(int i = 0; i < m.rowSize(); i++)
                for(int j = 0; j < m.columnSize(); j++)
                    assertTrue("Unexpected value.", Double.compare(m.get(i, j), 0d) == 0);
        });
    }

    /** */
    @Test
    public void testMapMatrix(){
        consumeSampleMatrix((m, desc)->{
            if (ignore(m.getClass()))
                return;

            double[][] doubles = fillAndReturn(m);

            Matrix cp = m.copy();

            m.map(cp, (m1, m2) -> m1 + m2);

            for(int i = 0; i < m.rowSize(); i++)
                for(int j = 0; j < m.columnSize(); j++)
                    assertEquals("Unexpected value.", m.get(i, j), doubles[i][j] * 2, 0d);
        });
    }

    /** */
    @Test
    public void testViewRow(){
        consumeSampleMatrix((m, desc)->{
            if (ignore(m.getClass()))
                return;

            fillMatrix(m);

            for (int i = 0; i < m.rowSize(); i++) {
                Vector vector = m.viewRow(i);
                assert vector != null;

                for (int j = 0; j < m.columnSize(); j++)
                    assertEquals("Unexpected value for " + desc, m.get(i, j), vector.get(j), 0d);
            }
        });
    }

    /** */
    @Test
    public void testViewCol(){
        consumeSampleMatrix((m, desc)->{
            if (ignore(m.getClass()))
                return;

            fillMatrix(m);

            for (int i = 0; i < m.columnSize(); i++) {
                Vector vector = m.viewColumn(i);
                assert vector != null;

                for (int j = 0; j < m.rowSize(); j++)
                    assertEquals("Unexpected value for " + desc, m.get(j, i), vector.get(j), 0d);
            }
        });
    }

    /** */
    @Test
    public void testFoldRow(){
        consumeSampleMatrix((m, desc)->{
            if (ignore(m.getClass()))
                return;

            fillMatrix(m);

            Vector foldRows = m.foldRows(Vector::sum);

            for (int i = 0; i < m.rowSize(); i++){
                Double locSum = 0d;

                for (int j = 0; j < m.columnSize(); j++)
                    locSum+=m.get(i, j);

                assertEquals("Unexpected value for " + desc, foldRows.get(i), locSum, 0d);
            }
        });
    }

    /** */
    @Test
    public void testFoldCol(){
        consumeSampleMatrix((m, desc)->{
            if (ignore(m.getClass()))
                return;

            fillMatrix(m);

            Vector foldCols = m.foldColumns(Vector::sum);

            for (int j = 0; j < m.columnSize(); j++){
                Double locSum = 0d;

                for (int i = 0; i < m.rowSize(); i++)
                    locSum+=m.get(i, j);

                assertEquals("Unexpected value for " + desc, foldCols.get(j), locSum, 0d);
            }
        });
    }

    /** */
    @Test
    public void testSum(){
        consumeSampleMatrix((m, desc)->{
            double[][] arr = fillAndReturn(m);

            double sum = m.sum();

            double rawSum = 0;
            for(int i = 0; i < arr.length; i++)
                for(int j = 0; j < arr[0].length; j++)
                    rawSum += arr[i][j];

            assertTrue("Unexpected value.", Double.compare(sum, rawSum) == 0);
        });
    }

    /** */
    @Test
    public void testMax(){
        consumeSampleMatrix((m, desc)->{
            double[][] doubles = fillAndReturn(m);
            double max = Double.NEGATIVE_INFINITY;

            for(int i = 0; i < m.rowSize(); i++)
                for(int j = 0; j < m.columnSize(); j++)
                    max = max < doubles[i][j] ? doubles[i][j] : max;

            assertEquals("Unexpected value for " + desc, m.maxValue().get(), max, 0d);
        });
    }

    /** */
    @Test
    public void testMin(){
        consumeSampleMatrix((m, desc)->{
            double[][] doubles = fillAndReturn(m);
            double min = Double.MAX_VALUE;

            for(int i = 0; i < m.rowSize(); i++)
                for(int j = 0; j < m.columnSize(); j++)
                    min = min > doubles[i][j] ? doubles[i][j] : min;

            assertEquals("Unexpected value for " + desc, m.minValue().get(), min, 0d);
        });
    }

    /** */
    private double[][] fillIntAndReturn(Matrix m) {
        double[][] arr = new double[m.rowSize()][m.columnSize()];

        if (m instanceof RandomMatrix){
            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    arr[i][j] = m.get(i, j);

        } else {
            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    arr[i][j] = i * m.rowSize() + j;

            m.assign(arr);
        }
        return arr;
    }

    /** */
    private double[][] fillAndReturn(Matrix m) {
        double[][] arr = new double[m.rowSize()][m.columnSize()];

        if (m instanceof RandomMatrix){
            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    arr[i][j] = m.get(i, j);

        } else {
            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    arr[i][j] = -0.5d + Math.random();

            m.assign(arr);
        }
        return arr;
    }

    private double recDet(int[] idxX, int[] idxY, Matrix origin, IntIntToDoubleFunction getter){
        int rows = idxX == null ? origin.rowSize() : idxX.length;
        int cols = idxX == null ? origin.columnSize() : idxX.length;

        if (rows != cols)
            throw new CardinalityException(rows, cols);

        if (rows == 1)
            return getter.apply(0, 0);

        if (rows == 2)
            return getter.apply(0, 0) * getter.apply(1, 1) - getter.apply(0, 1) * getter.apply(1, 0);

        if (rows == 3)
            return getter.apply(0, 0) * (getter.apply(1, 1) * getter.apply(2, 2) - getter.apply(1, 2) * getter.apply(2, 1))
                - getter.apply(0, 1) * (getter.apply(1, 0) * getter.apply(2, 2) - getter.apply(1, 2) * getter.apply(2, 0))
                + getter.apply(0, 2) * (getter.apply(1, 0) * getter.apply(2, 1) - getter.apply(1, 1) * getter.apply(2, 0));

        if (idxX == null){
            idxX = IntStream.range(0, rows).toArray();
            idxY = IntStream.range(0, cols).toArray();
        }

        double det = 0;

        for (int i = 0; i < rows; i++) {
            int[] finalIdxX = skipIdx(idxX, 0);
            int[] finalIdxY = skipIdx(idxY, i);

            IntIntToDoubleFunction get = (x, y) -> origin.getX(finalIdxX[x], finalIdxY[y]);

            det += Math.pow(-1, i) * origin.getX(finalIdxX[0], finalIdxY[i]) * recDet(finalIdxX, finalIdxY, origin, get);
        }

        return det;
    }

    private int[] skipIdx(int[] idxs, int idx){
        int[] res = new int[idxs.length -1];
        int j = 0;

        for (int i = 0; i < idxs.length; i++)
            if (i != idx)
                res[j++] = idxs[i];

        return res;
    }

    /** */
    private void fillMatrix(Matrix m){
        for (int i = 0; i < m.rowSize(); i++)
            for (int j = 0; j < m.columnSize(); j++)
                m.set(i, j, Math.random());
    }

    /** Ignore test for given vector type. */
    private boolean ignore(Class<? extends Matrix> clazz){
        boolean isIgnored = false;
        List<Class<? extends Matrix>> ignoredClasses = Arrays.asList(RandomMatrix.class);

        for (Class<? extends Matrix> ignoredClass : ignoredClasses) {
            if (ignoredClass.isAssignableFrom(clazz)){
                isIgnored = true;
                break;
            }
        }

        return isIgnored;
    }

    /** */
    private static Map<Class<? extends Matrix>, Class<? extends Vector>> typesMap() {
        return new LinkedHashMap<Class<? extends Matrix>, Class<? extends Vector>> () {{
            put(DenseLocalOnHeapMatrix.class, DenseLocalOnHeapVector.class);
            put(DenseLocalOffHeapMatrix.class, DenseLocalOffHeapVector.class);
            put(RandomMatrix.class, RandomVector.class);
            // IMPL NOTE check for presence of all implementations here will be done in testHaveLikeMatrix via Fixture
        }};
    }
}
