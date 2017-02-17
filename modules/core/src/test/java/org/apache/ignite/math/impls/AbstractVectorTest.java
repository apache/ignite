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

package org.apache.ignite.math.impls;

import org.apache.ignite.lang.*;
import org.apache.ignite.math.*;
import org.apache.ignite.math.UnsupportedOperationException;
import org.apache.ignite.math.Vector;
import org.apache.ignite.math.impls.storage.*;
import org.junit.*;
import java.util.*;
import java.util.stream.*;

import static java.util.Spliterator.*;
import static org.apache.ignite.math.impls.MathTestConstants.*;
import static org.junit.Assert.*;

/**
 * Unit test for {@link AbstractVector}.
 */
public class AbstractVectorTest {
    /** */
    private AbstractVector testVector;

    /** */
    @Before
    public void setUp() {
        testVector = getAbstractVector();
    }

    /** */
    @Test
    public void setStorage() {
        assertTrue(testVector.size() == 0);

        testVector.setStorage(createStorage());

        assertTrue(testVector.size() == STORAGE_SIZE);
    }

    /** */
    @Test
    public void size() {
        assertTrue(testVector.size() == 0);

        testVector.setStorage(createStorage());
        assertTrue(testVector.size() == STORAGE_SIZE);

        testVector.setStorage(new VectorArrayStorage(STORAGE_SIZE + STORAGE_SIZE));
        assertTrue(testVector.size() == STORAGE_SIZE + STORAGE_SIZE);

        testVector = getAbstractVector(createStorage());
        assertTrue(testVector.size() == STORAGE_SIZE);
    }

    /** */
    @Test
    public void getPositive() {
        testVector.setStorage(createStorage());

        for (int i = 0; i < STORAGE_SIZE; i++)
            assertNotNull(NULL_VALUES, testVector.get(i));

    }

    /** */
    @Test(expected = IndexException.class)
    public void getNegative0() {
        testVector.get(0);
    }

    /** */
    @Test(expected = IndexException.class)
    public void getNegative1() {
        testVector.setStorage(createStorage());

        testVector.get(-1);
    }

    /** */
    @Test(expected = IndexException.class)
    public void getNegative2() {
        testVector.setStorage(createStorage());

        testVector.get(testVector.size() + 1);
    }

    /** */
    @Test
    public void getX() {
        VectorStorage storage = createStorage();

        testVector.setStorage(storage);

        for (int i = 0; i < STORAGE_SIZE; i++)
            assertEquals(VALUE_NOT_EQUALS, testVector.get(i), storage.get(i), NIL_DELTA);
    }

    /** */
    @Test(expected = UnsupportedOperationException.class)
    public void getXNegative0() {
        testVector.getX(0);
    }

    /** */
    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void getXNegative1() {
        testVector.setStorage(createStorage());

        testVector.getX(-1);
    }

    /** */
    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void getXNegative2() {
        testVector.setStorage(createStorage());

        testVector.getX(STORAGE_SIZE + 1);
    }

    /** */
    @Test
    public void mapTwoVectors() {
        VectorStorage storage = createStorage();

        double[] data = storage.data().clone();

        testVector.setStorage(storage);

        AbstractVector testVector1 = getAbstractVector(storage);

        Vector map = testVector.map(testVector1, Functions.PLUS);

        for (int i = 0; i < STORAGE_SIZE; i++)
            assertEquals(VALUE_NOT_EQUALS, map.get(i), data[i] + data[i], NIL_DELTA);
    }

    /** */
    @Test
    public void mapDoubleFunc() {
        double[] data = initVector();
        Vector map = testVector.map(Functions.INV);

        for (int i = 0; i < STORAGE_SIZE; i++)
            assertEquals(VALUE_NOT_EQUALS, map.get(i), Functions.INV.apply(data[i]), NIL_DELTA);
    }

    /** */
    @Test
    public void mapCurrying() {
        double[] data = initVector();
        Vector map = testVector.map(Functions.PLUS, SECOND_ARG);

        for (int i = 0; i < STORAGE_SIZE; i++)
            assertEquals(VALUE_NOT_EQUALS, map.get(i), Functions.PLUS.apply(data[i], SECOND_ARG), NIL_DELTA);
    }

    /** */
    @Test
    public void minValue() {
        double[] data = initVector();

        Vector.Element minVal = testVector.minValue();

        assertEquals(VALUE_NOT_EQUALS, minVal.get(), Arrays.stream(data).min().getAsDouble(), NIL_DELTA);
    }

    /** */
    @Test
    public void maxValue() {
        double[] data = initVector();

        Vector.Element maxVal = testVector.maxValue();

        assertEquals(VALUE_NOT_EQUALS, maxVal.get(), Arrays.stream(data).max().getAsDouble(), NIL_DELTA);
    }

    /** */
    @Test
    public void set() {
        double[] data = initVector();

        for (int i = 0; i < STORAGE_SIZE; i++)
            testVector.set(i, Math.exp(data[i]));

        for (int i = 0; i < STORAGE_SIZE; i++)
            assertEquals(VALUE_NOT_EQUALS, testVector.get(i), Math.exp(data[i]), NIL_DELTA);
    }

    /** */
    @Test(expected = IndexException.class)
    public void setNegative0() {
        testVector.set(-1, -1);
    }

    /** */
    @Test(expected = IndexException.class)
    public void setNegative1() {
        initVector();

        testVector.set(-1, -1);
    }

    /** */
    @Test(expected = IndexException.class)
    public void setNegative2() {
        initVector();

        testVector.set(STORAGE_SIZE + 1, -1);
    }

    /** */
    @Test
    public void setX() {
        double[] data = initVector();

        for (int i = 0; i < STORAGE_SIZE; i++)
            testVector.setX(i, Math.exp(data[i]));

        for (int i = 0; i < STORAGE_SIZE; i++)
            assertEquals(VALUE_NOT_EQUALS, testVector.get(i), Math.exp(data[i]), NIL_DELTA);
    }

    /** */
    @Test(expected = IndexOutOfBoundsException.class)
    public void setXNegative0() {
        initVector();

        testVector.setX(-1, -1);
    }

    /** */
    @Test(expected = IndexOutOfBoundsException.class)
    public void setXNegative1() {
        initVector();

        testVector.setX(STORAGE_SIZE + 1, -1);
    }

    /** */
    @Test(expected = UnsupportedOperationException.class)
    public void setXNegative2() {
        testVector.setX(-1, -1);
    }

    /** */
    @Test
    public void increment() {
        double[] data = initVector();

        for (int i = 0; i < STORAGE_SIZE; i++)
            testVector.increment(i, 1d);

        for (int i = 0; i < STORAGE_SIZE; i++)
            assertEquals(VALUE_NOT_EQUALS, testVector.get(i), data[i] + 1, NIL_DELTA);
    }

    /** */
    @Test
    public void incrementX() {
        double[] data = initVector();

        for (int i = 0; i < STORAGE_SIZE; i++)
            testVector.incrementX(i, 1d);

        for (int i = 0; i < STORAGE_SIZE; i++)
            assertEquals(VALUE_NOT_EQUALS, testVector.get(i), data[i] + 1, NIL_DELTA);
    }

    /** */
    @Test
    public void isZero() {
        assertTrue(UNEXPECTED_VALUE, testVector.isZero(0d));

        assertFalse(UNEXPECTED_VALUE, testVector.isZero(1d));
    }

    /** */
    @Test
    public void sum() {
        double[] data = initVector();

        assertEquals(VALUE_NOT_EQUALS, testVector.sum(), Arrays.stream(data).sum(), EXPECTED_DELTA);
    }

    /** */
    @Test
    public void guid() {
        assertNotNull(NULL_GUID, testVector.guid());

        assertEquals(UNEXPECTED_GUID_VALUE, testVector.guid(), testVector.guid());

        assertFalse(EMPTY_GUID, testVector.guid().toString().isEmpty());

        testVector = getAbstractVector(createStorage());

        assertNotNull(NULL_GUID, testVector.guid());

        assertEquals(UNEXPECTED_GUID_VALUE, testVector.guid(), testVector.guid());

        assertFalse(EMPTY_GUID, testVector.guid().toString().isEmpty());
    }

    /** */
    @Test
    public void equalsTest() {
        VectorStorage storage = createStorage();

        AbstractVector testVector1 = getAbstractVector();

        testVector1.setStorage(storage);

        AbstractVector testVector2 = getAbstractVector();

        assertEquals(VALUE_NOT_EQUALS, testVector, testVector);

        assertEquals(VALUE_NOT_EQUALS, testVector, testVector2);

        assertEquals(VALUE_NOT_EQUALS, testVector1, testVector1);

        testVector2.setStorage(storage);

        assertTrue(VALUE_NOT_EQUALS, testVector1.equals(testVector2));

        assertFalse(VALUES_SHOULD_BE_NOT_EQUALS, testVector.equals(testVector1));

        assertFalse(VALUES_SHOULD_BE_NOT_EQUALS, testVector1.equals(testVector));

        testVector1.setStorage(null);

        assertTrue(VALUE_NOT_EQUALS, testVector.equals(testVector1));
    }

    /** */
    @Test(expected = NoSuchElementException.class)
    public void all() {
        assertNotNull(NULL_VALUE, testVector.all());

        assertNotNull(NULL_VALUE, getAbstractVector(createStorage()).all());

        getAbstractVector().all().iterator().next();
    }

    /** */
    @Test
    public void hashCodeTest() {
        IgniteUuid guid = testVector.guid();

        assertEquals(VALUE_NOT_EQUALS, testVector.hashCode(), guid.hashCode());
    }

    /** */
    @Test
    public void nonZeroElements() {
        VectorStorage storage = createStorage();

        double[] data = storage.data();

        assertEquals(VALUE_NOT_EQUALS, testVector.nonZeroElements(), 0);

        testVector.setStorage(storage);

        assertEquals(VALUE_NOT_EQUALS, testVector.nonZeroElements(), Arrays.stream(data).filter(x -> x != 0d).count());

        addNilValues(data);

        assertEquals(VALUE_NOT_EQUALS, testVector.nonZeroElements(), Arrays.stream(data).filter(x -> x != 0d).count());
    }

    /** */
    @Test
    public void clusterGroup() {

        assertNull(NOT_NULL_VALUE, testVector.clusterGroup());

    }

    /** */
    @Test
    public void foldMapWithSecondVector() {
        double[] data0 = initVector();

        VectorStorage storage1 = createStorage();

        double[] data1 = storage1.data().clone();

        AbstractVector testVector1 = getAbstractVector(storage1);

        assertEquals(VALUE_NOT_EQUALS, testVector.foldMap(testVector1, Functions.PLUS, Functions.PLUS, 0d), Arrays.stream(data0).sum() + Arrays.stream(data1).sum(), EXPECTED_DELTA);

        String testVal = "";

        for (int i = 0; i < data0.length; i++)
            testVal += data0[i] + data1[i];

        assertEquals(VALUE_NOT_EQUALS, testVector.foldMap(testVector1, (string, xi) -> string.concat(xi.toString()), Functions.PLUS, ""), testVal);

    }

    /** */
    @Test
    public void foldMap() {
        double[] data = initVector();

        assertEquals(VALUE_NOT_EQUALS, testVector.foldMap(Functions.PLUS, Math::sin, 0d), Arrays.stream(data).map(Math::sin).sum(), EXPECTED_DELTA);
    }

    /** */
    @Test
    public void nonZeroes() {
        assertNotNull(NULL_VALUE, testVector.nonZeroes());

        VectorStorage storage = createStorage();

        double[] data = storage.data();

        testVector.setStorage(storage);

        assertNotNull(NULL_VALUE, testVector.nonZeroes());

        assertEquals(VALUE_NOT_EQUALS, StreamSupport.stream(testVector.nonZeroes().spliterator(), false).count(), Arrays.stream(data).filter(x -> x != 0d).count());

        addNilValues(data);

        assertEquals(VALUE_NOT_EQUALS, StreamSupport.stream(testVector.nonZeroes().spliterator(), false).count(), Arrays.stream(data).filter(x -> x != 0d).count());
    }

    /** */
    @Test(expected = NoSuchElementException.class)
    public void nonZeroesEmpty() {
        testVector.nonZeroes().iterator().next();
    }

    /** */
    @Test(expected = UnsupportedOperationException.class)
    public void assign() {
        testVector.assign(TEST_VALUE);
    }

    /** */
    @Test
    public void assignPositive() {
        initVector();

        testVector.assign(TEST_VALUE);

        testVector.all().forEach(x -> assertTrue(UNEXPECTED_VALUE, x.get() == TEST_VALUE));
    }

    /** */
    @Test(expected = CardinalityException.class)
    public void assignArr() {
        testVector.assign(new double[1]);
    }

    /** */
    @Test(expected = UnsupportedOperationException.class)
    public void assignArrEmpty() {
        testVector.assign(new double[0]);
    }

    /** */
    @Test
    public void assignArrPositive() {
        double[] doubles = initVector();

        doubles = Arrays.stream(doubles).map(x -> x + x).toArray();

        testVector.assign(doubles);

        for (int i = 0; i < STORAGE_SIZE; i++)
            assertEquals(VALUE_NOT_EQUALS, testVector.get(i), doubles[i], NIL_DELTA);

    }

    /** */
    @Test
    public void assignByFunc() {

        for (Vector.Element x : testVector.all())
            assertNotEquals(VALUES_SHOULD_BE_NOT_EQUALS, x.index(), x.get(), NIL_DELTA);

        testVector.setStorage(createEmptyStorage());
        testVector.assign(x -> x);

        for (Vector.Element x : testVector.all())
            assertEquals(VALUE_NOT_EQUALS, x.index(), x.get(), NIL_DELTA);
    }

    /** */
    @Test
    public void assign3() {
        AbstractVector testVector1 = getAbstractVector(createStorage());

        testVector = getAbstractVector(createEmptyStorage());

        testVector.assign(testVector1);

        assertTrue(VALUE_NOT_EQUALS, testVector.equals(testVector1));
    }

    /** */
    @Test
    public void cross() { // TODO write test

    }

    /** */
    @Test
    public void allSpliterator() {

        Spliterator<Double> spliterator = testVector.allSpliterator();

        assertNotNull(NULL_VALUE, spliterator);

        assertEquals(VALUE_NOT_EQUALS, spliterator.estimateSize(), 0);

        assertEquals(VALUE_NOT_EQUALS, spliterator.getExactSizeIfKnown(), 0);

        assertNull(NOT_NULL_VALUE, spliterator.trySplit());

        assertTrue(UNEXPECTED_VALUE, spliterator.hasCharacteristics(ORDERED | SIZED));

        double[] data = initVector();

        spliterator = testVector.allSpliterator();

        assertNotNull(NULL_VALUE, spliterator);

        assertEquals(VALUE_NOT_EQUALS, spliterator.estimateSize(), data.length);

        assertEquals(VALUE_NOT_EQUALS, spliterator.getExactSizeIfKnown(), data.length);

        assertTrue(UNEXPECTED_VALUE, spliterator.hasCharacteristics(ORDERED | SIZED));

        Spliterator<Double> secondHalf = spliterator.trySplit();

        assertNull(NOT_NULL_VALUE, secondHalf);

        spliterator.tryAdvance(x -> {
        });
    }

    /** */
    @Test
    public void nonZeroSpliterator() {
        Spliterator<Double> spliterator = testVector.nonZeroSpliterator();

        assertNotNull(NULL_VALUE, spliterator);

        assertEquals(VALUE_NOT_EQUALS, spliterator.estimateSize(), 0);

        assertEquals(VALUE_NOT_EQUALS, spliterator.getExactSizeIfKnown(), 0);

        assertNull(NOT_NULL_VALUE, spliterator.trySplit());

        assertTrue(UNEXPECTED_VALUE, spliterator.hasCharacteristics(ORDERED | SIZED));

        double[] data = initVector();

        spliterator = testVector.nonZeroSpliterator();

        assertNotNull(NULL_VALUE, spliterator);

        assertEquals(VALUE_NOT_EQUALS, spliterator.estimateSize(), data.length);

        assertEquals(VALUE_NOT_EQUALS, spliterator.getExactSizeIfKnown(), data.length);

        assertTrue(UNEXPECTED_VALUE, spliterator.hasCharacteristics(ORDERED | SIZED));

        Spliterator<Double> secondHalf = spliterator.trySplit();

        assertNull(NOT_NULL_VALUE, secondHalf);

        addNilValues(data);

        spliterator = testVector.nonZeroSpliterator();

        assertNotNull(NULL_VALUE, spliterator);

        assertEquals(VALUE_NOT_EQUALS, spliterator.estimateSize(), Arrays.stream(data).filter(x -> x != 0d).count());

        assertEquals(VALUE_NOT_EQUALS, spliterator.getExactSizeIfKnown(), Arrays.stream(data).filter(x -> x != 0d).count());

        assertTrue(UNEXPECTED_VALUE, spliterator.hasCharacteristics(ORDERED | SIZED));

        secondHalf = spliterator.trySplit();

        assertNull(NOT_NULL_VALUE, secondHalf);

        if (!spliterator.tryAdvance(x -> {
        }))
            fail(NO_NEXT_ELEMENT);
    }

    /** */
    @Test
    public void dot() {
        assertEquals(VALUE_NOT_EQUALS, testVector.dot(testVector), 0d, NIL_DELTA);

        double[] data = initVector();

        assertEquals(VALUE_NOT_EQUALS, testVector.dot(testVector), Arrays.stream(data).reduce(0, (x, y) -> x + y * y), NIL_DELTA);
    }

    /** */
    @Test(expected = CardinalityException.class)
    public void dotNegative() {
        testVector.dot(getAbstractVector(createEmptyStorage()));
    }

    /** */
    @Test
    public void dotSelf() {
        assertEquals(VALUE_NOT_EQUALS, testVector.dotSelf(), 0d, NIL_DELTA);

        double[] data = initVector();

        assertEquals(VALUE_NOT_EQUALS, testVector.dotSelf(), Arrays.stream(data).reduce(0, (x, y) -> x + y * y), NIL_DELTA);
    }

    /** */
    @Test
    public void plus(){
        double[] data0 = initVector();

        VectorStorage storage = createStorage();

        double[] data1 = storage.data().clone();

        AbstractVector testVector1 = getAbstractVector();

        testVector1.setStorage(storage);

        Vector plus = testVector.plus(testVector1);

        for (int i = 0; i < data0.length; i++) {
            data0[i] += data1[i];

            assertEquals(VALUE_NOT_EQUALS, plus.get(i), data0[i], NIL_DELTA);
        }
    }

    /** */
    @Test
    public void plusDouble(){
        double[] data0 = initVector();

        Vector plus = testVector.plus(TEST_VALUE);

        for (int i = 0; i < data0.length; i++) {
            data0[i] += TEST_VALUE;

            assertEquals(VALUE_NOT_EQUALS, plus.get(i), data0[i], NIL_DELTA);
        }
    }

    /** */
    @Test
    public void minus(){
        double[] data0 = initVector();

        VectorStorage storage1 = createStorage();

        double[] data1 = storage1.data().clone();

        AbstractVector testVector1 = getAbstractVector();

        testVector1.setStorage(storage1);

        Vector minus = testVector.minus(testVector1);

        for (int i = 0; i < data0.length; i++) {
            data0[i] -= data1[i];

            assertEquals(VALUE_NOT_EQUALS, minus.get(i), data0[i], NIL_DELTA);
        }
    }

    /** */
    @Test
    public void times(){
        double[] data0 = initVector();

        VectorStorage storage1 = createStorage();

        double[] data1 = storage1.data().clone();

        AbstractVector testVector1 = getAbstractVector();

        testVector1.setStorage(storage1);

        Vector times = testVector.times(testVector1);

        for (int i = 0; i < data0.length; i++) {
            data0[i] *= data1[i];

            assertEquals(VALUE_NOT_EQUALS, times.get(i), data0[i], NIL_DELTA);
        }
    }

    /** */
    @Test
    public void timesDouble(){
        double[] data0 = initVector();

        double testVal = 2.5;

        Vector times = testVector.times(testVal);

        for (int i = 0; i < data0.length; i++) {
            data0[i] *= testVal;

            assertEquals(VALUE_NOT_EQUALS, times.get(i), data0[i], NIL_DELTA);
        }
    }

    /** */
    @Test
    public void getStorage(){
        assertTrue(UNEXPECTED_VALUE, testVector.getStorage().getClass() == VectorNullStorage.class);
        assertNotNull(NULL_VALUE, getAbstractVector(createEmptyStorage()));
        assertNotNull(NULL_VALUE, getAbstractVector(createStorage()));
        testVector.setStorage(createStorage());
        assertNotNull(NULL_VALUE, testVector.getStorage());
    }

    /** */
    @Test
    public void divide(){
        double[] data0 = initVector();

        double testVal = 2.5;

        Vector times = testVector.divide(testVal);

        for (int i = 0; i < data0.length; i++) {
            data0[i] /= testVal;

            assertEquals(VALUE_NOT_EQUALS, times.get(i), data0[i], NIL_DELTA);
        }
    }

    /** */
    @Test
    public void getElement() {
        double[] data = initVector();

        for (int i = 0; i < data.length; i++) {
            assertNotNull(NULL_VALUE, testVector.getElement(i));

            assertEquals(UNEXPECTED_VALUE, testVector.getElement(i).get(), data[i], NIL_DELTA);

            testVector.getElement(i).set(++data[i]);

            assertEquals(UNEXPECTED_VALUE, testVector.getElement(i).get(), data[i], NIL_DELTA);
        }
    }

    /**
     * Create {@link AbstractVector} with storage for tests.
     *
     * @param storage {@link VectorStorage}
     * @return AbstractVector.
     */
    private AbstractVector getAbstractVector(VectorStorage storage) {
        return new AbstractVector(storage) { // TODO: find out how to fix warning about missing constructor
            /** */
            @Override public boolean isDense() {
                return false;
            }

            @Override
            public Matrix toMatrix(boolean rowLike) {
                return null; // TODO
            }

            @Override
            public Matrix toMatrixPlusOne(boolean rowLike, double zeroVal) {
                return null; // TODO
            }

            /** */
            @Override public boolean isSequentialAccess() {
                return false;
            }

            /** */
            @Override public Matrix likeMatrix(int rows, int cols) {
                return null;
            }

            /** */
            @Override public Vector copy() {
                return getAbstractVector(this.getStorage());
            }

            /** */
            @Override public Vector like(int crd) {
                return null;
            }

            /** */
            @Override public Vector normalize() {
                return null;
            }

            /** */
            @Override public Vector normalize(double power) {
                return null;
            }

            /** */
            @Override public Vector logNormalize() {
                return null;
            }

            /** */
            @Override public Vector logNormalize(double power) {
                return null;
            }

            /** */
            @Override public Vector viewPart(int off, int len) {
                return null;
            }

            /** */
            @Override public Matrix cross(Vector vec) {
                return null;
            }

            /** */
            @Override public double getLookupCost() {
                return 0;
            }

            /** */
            @Override public boolean isAddConstantTime() {
                return false;
            }
        };
    }

    /**
     * Create empty {@link AbstractVector} for tests.
     *
     * @return AbstractVector.
     */
    private AbstractVector getAbstractVector() {
        return new AbstractVector() { // TODO: find out how to fix warning about missing constructor
            /** */
            @Override public boolean isDense() {
                return false;
            }

            @Override
            public Matrix toMatrix(boolean rowLike) {
                return null; // TODO
            }

            @Override
            public Matrix toMatrixPlusOne(boolean rowLike, double zeroVal) {
                return null; // TODO
            }

            /** */
            @Override public Matrix likeMatrix(int rows, int cols) {
                return null;
            }

            /** */
            @Override public boolean isSequentialAccess() {
                return false;
            }

            /** */
            @Override public Vector copy() {
                return getAbstractVector(this.getStorage());
            }

            /** */
            @Override public Vector like(int crd) {
                return null;
            }

            /** */
            @Override public Vector normalize() {
                return null;
            }

            /** */
            @Override public Vector normalize(double power) {
                return null;
            }

            /** */
            @Override public Vector logNormalize() {
                return null;
            }

            /** */
            @Override public Vector logNormalize(double power) {
                return null;
            }

            /** */
            @Override public Vector viewPart(int off, int len) {
                return null;
            }

            /** */
            @Override public Matrix cross(Vector vec) {
                return null;
            }

            /** */
            @Override public double getLookupCost() {
                return 0;
            }

            /** */
            @Override public boolean isAddConstantTime() {
                return false;
            }
        };
    }

    /**
     * Create {@link VectorStorage} for tests.
     *
     * @return VectorStorage
     */
    private VectorStorage createEmptyStorage() {
        return new VectorArrayStorage(STORAGE_SIZE);
    }

    /**
     * Create filled {@link VectorStorage} for tests.
     *
     * @return VectorStorage.
     */
    private VectorStorage createStorage() {
        VectorArrayStorage storage = new VectorArrayStorage(STORAGE_SIZE);

        for (int i = 0; i < STORAGE_SIZE; i++)
            storage.set(i, Math.random());

        return storage;
    }

    /**
     * Init vector and return initialized values.
     *
     * @return Initial values.
     */
    private double[] initVector() {
        VectorStorage storage = createStorage();
        double[] data = storage.data().clone();

        testVector.setStorage(storage);
        return data;
    }

    /**
     * Add some zeroes to vector elements.
     */
    private void addNilValues() {
        testVector.set(10, 0);
        testVector.set(50, 0);
    }

    /**
     * Add some zeroes to vector elements. Also set zeroes to the same elements in reference array data
     *
     * @param testRef
     */
    private void addNilValues(double[] testRef) {
        addNilValues();
        testRef[10] = 0;
        testRef[50] = 0;
    }
}
