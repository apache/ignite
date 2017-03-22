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

package org.apache.ignite.math.impls.vector;

import org.apache.ignite.lang.*;
import org.apache.ignite.math.*;
import org.apache.ignite.math.Vector;
import org.apache.ignite.math.exceptions.*;
import org.apache.ignite.math.functions.*;
import org.apache.ignite.math.impls.storage.vector.*;
import org.junit.*;
import java.util.*;
import java.util.stream.*;

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
        testVector.setStorage(createStorage());

        assertTrue(testVector.size() == STORAGE_SIZE);
    }

    /** */
    @Test
    public void size() {
        testVector.setStorage(createStorage());
        assertTrue(testVector.size() == STORAGE_SIZE);

        testVector.setStorage(new ArrayVectorStorage(STORAGE_SIZE + STORAGE_SIZE));
        assertTrue(testVector.size() == STORAGE_SIZE + STORAGE_SIZE);

        testVector = getAbstractVector(createStorage());
        assertTrue(testVector.size() == STORAGE_SIZE);
    }

    /** */
    @Test
    public void getPositive() {
        testVector = getAbstractVector(createStorage());

        for (int i = 0; i < STORAGE_SIZE; i++)
            assertNotNull(NULL_VALUES, testVector.get(i));

    }

    /** */
    @Test(expected = NullPointerException.class)
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
    @Test(expected = NullPointerException.class)
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
    @Test(expected = NullPointerException.class)
    public void setXNegative2() {
        testVector.setX(-1, -1);
    }

    /** */
    @Test
    public void isZero() {
        assertTrue(UNEXPECTED_VALUE, testVector.isZero(0d));

        assertFalse(UNEXPECTED_VALUE, testVector.isZero(1d));
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

        testVector2.setStorage(storage);

        assertTrue(VALUE_NOT_EQUALS, testVector1.equals(testVector2));

        assertFalse(VALUES_SHOULD_BE_NOT_EQUALS, testVector1.equals(testVector));
    }

    /** */
    @Test(expected = NullPointerException.class)
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

        testVector = getAbstractVector(storage);

        assertEquals(VALUE_NOT_EQUALS, testVector.nonZeroElements(), Arrays.stream(data).filter(x -> x != 0d).count());

        addNilValues(data);

        assertEquals(VALUE_NOT_EQUALS, testVector.nonZeroElements(), Arrays.stream(data).filter(x -> x != 0d).count());
    }

    /** */
    @Test
    public void foldMapWithSecondVector() {
        double[] data0 = initVector();

        VectorStorage storage1 = createStorage();

        double[] data1 = storage1.data().clone();

        AbstractVector testVector1 = getAbstractVector(storage1);

        String testVal = "";

        for (int i = 0; i < data0.length; i++)
            testVal += data0[i] + data1[i];

        assertEquals(VALUE_NOT_EQUALS, testVector.foldMap(testVector1, (string, xi) -> string.concat(xi.toString()), Functions.PLUS, ""), testVal);
    }

    /** */
    @Test
    public void nonZeroes() {
        assertNotNull(NULL_VALUE, testVector.nonZeroes());

        double[] data = initVector();

        assertNotNull(NULL_VALUE, testVector.nonZeroes());

        assertEquals(VALUE_NOT_EQUALS, StreamSupport.stream(testVector.nonZeroes().spliterator(), false).count(), Arrays.stream(data).filter(x -> x != 0d).count());

        addNilValues(data);

        assertEquals(VALUE_NOT_EQUALS, StreamSupport.stream(testVector.nonZeroes().spliterator(), false).count(), Arrays.stream(data).filter(x -> x != 0d).count());
    }

    /** */
    @Test(expected = NullPointerException.class)
    public void nonZeroesEmpty() {
        testVector.nonZeroes().iterator().next();
    }

    /** */
    @Test(expected = NullPointerException.class)
    public void assign() {
        testVector.assign(TEST_VALUE);
    }

    /** */
    @Test(expected = NullPointerException.class)
    public void assignArr() {
        testVector.assign(new double[1]);
    }

    /** */
    @Test(expected = NullPointerException.class)
    public void assignArrEmpty() {
        testVector.assign(new double[0]);
    }

    /** */
    @Test(expected = NullPointerException.class)
    public void dotNegative() {
        testVector.dot(getAbstractVector(createEmptyStorage()));
    }

    /** */
    @Test
    public void dotSelf() {
        double[] data = initVector();

        assertEquals(VALUE_NOT_EQUALS, testVector.dotSelf(), Arrays.stream(data).reduce(0, (x, y) -> x + y * y), NIL_DELTA);
    }

    /** */
    @Test
    public void getStorage(){
        assertNotNull(NULL_VALUE, getAbstractVector(createEmptyStorage()));
        assertNotNull(NULL_VALUE, getAbstractVector(createStorage()));
        testVector.setStorage(createStorage());
        assertNotNull(NULL_VALUE, testVector.getStorage());
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

            /** {@inheritDoc} */
            @Override public boolean isRandomAccess() {
                return true;
            }

            /** {@inheritDoc} */
            @Override public boolean isDistributed() {
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

            /** {@inheritDoc} */
            @Override public boolean isRandomAccess() {
                return true;
            }

            /** {@inheritDoc} */
            @Override public boolean isDistributed() {
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
        return new ArrayVectorStorage(STORAGE_SIZE);
    }

    /**
     * Create filled {@link VectorStorage} for tests.
     *
     * @return VectorStorage.
     */
    private VectorStorage createStorage() {
        ArrayVectorStorage storage = new ArrayVectorStorage(STORAGE_SIZE);

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

        testVector = getAbstractVector(storage);
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
     */
    private void addNilValues(double[] testRef) {
        addNilValues();
        testRef[10] = 0;
        testRef[50] = 0;
    }
}
