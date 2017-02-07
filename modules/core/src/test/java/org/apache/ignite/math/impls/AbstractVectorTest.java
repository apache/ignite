package org.apache.ignite.math.impls;

import org.apache.ignite.math.*;
import org.apache.ignite.math.Vector;
import org.junit.*;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Unit test for {@link AbstractVector}
 */
public class AbstractVectorTest {
    /** */
    private static final String VALUE_NOT_EQUALS = "value not equals";

    /** */
    private static final double SECOND_ARG = 1d;

    /**
     * we assume that we will check calculation precision in other tests
     */
    private static final double EXPECTED_DELTA = 0.1d;

    /** */
    private static final String UNEXPECTED_VALUE = "unexpected value";

    /** */
    private AbstractVector testVector;

    /** */
    @Before
    public void setUp() throws Exception {
        testVector = getAbstractVector();
    }

    /** */
    @Test
    public void setStorage() throws Exception {
        assertTrue(testVector.size() == 0);

        testVector.setStorage(createStorage());

        assertTrue(testVector.size() == VectorArrayStorageTest.STORAGE_SIZE);
    }

    /** */
    @Test
    public void size() throws Exception {
        assertTrue(testVector.size() == 0);

        testVector.setStorage(createStorage());
        assertTrue(testVector.size() == VectorArrayStorageTest.STORAGE_SIZE);

        testVector.setStorage(new VectorArrayStorage(VectorArrayStorageTest.STORAGE_SIZE + VectorArrayStorageTest.STORAGE_SIZE));
        assertTrue(testVector.size() == VectorArrayStorageTest.STORAGE_SIZE + VectorArrayStorageTest.STORAGE_SIZE);

        testVector = getAbstractVector(createStorage());
        assertTrue(testVector.size() == VectorArrayStorageTest.STORAGE_SIZE);
    }

    /** */
    @Test
    public void getPositive() throws Exception {
        testVector.setStorage(createStorage());

        for (int i = 0; i < VectorArrayStorageTest.STORAGE_SIZE; i++)
            assertNotNull("null values", testVector.get(i));

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
    @Test
    public void getX() throws Exception {
        VectorStorage storage = createStorage();

        testVector.setStorage(storage);

        for (int i = 0; i < VectorArrayStorageTest.STORAGE_SIZE; i++)
            assertEquals(VALUE_NOT_EQUALS, testVector.get(i), storage.get(i), VectorArrayStorageTest.NIL_DELTA);
    }

    /** */
    @Test(expected = NullPointerException.class)
    public void getXNegative0() throws Exception {
        testVector.getX(0);
    }

    /** */
    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void getXNegative1() throws Exception {
        testVector.setStorage(createStorage());

        testVector.getX(-1);
    }

    /** */
    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void getXNegative2() throws Exception {
        testVector.setStorage(createStorage());

        testVector.getX(VectorArrayStorageTest.STORAGE_SIZE + 1);
    }

    /** */
    @Test
    public void mapTwoVectors() throws Exception {
        VectorStorage storage = createStorage();
        double[] data = storage.data().clone();

        testVector.setStorage(storage);
        AbstractVector testVector1 = getAbstractVector(storage);

        Vector map = testVector.map(testVector1, Functions.PLUS);

        for (int i = 0; i < VectorArrayStorageTest.STORAGE_SIZE; i++)
            assertEquals(VALUE_NOT_EQUALS, map.get(i), data[i] + data[i], VectorArrayStorageTest.NIL_DELTA);
    }

    /** */
    @Test
    public void mapDoubleFunc() throws Exception {
        double[] data = initVector();
        Vector map = testVector.map(Functions.INV);

        for (int i = 0; i < VectorArrayStorageTest.STORAGE_SIZE; i++)
            assertEquals(VALUE_NOT_EQUALS, map.get(i), Functions.INV.apply(data[i]), VectorArrayStorageTest.NIL_DELTA);
    }

    /** */
    @Test
    public void mapCurrying() throws Exception {
        double[] data = initVector();
        Vector map = testVector.map(Functions.PLUS, SECOND_ARG);

        for (int i = 0; i < VectorArrayStorageTest.STORAGE_SIZE; i++)
            assertEquals(VALUE_NOT_EQUALS, map.get(i), Functions.PLUS.apply(data[i], SECOND_ARG), VectorArrayStorageTest.NIL_DELTA);
    }

    /** */
    @Test
    public void minValue() throws Exception {
        double[] data = initVector();

        Vector.Element minVal = testVector.minValue();

        assertEquals(VALUE_NOT_EQUALS, minVal.get(), Arrays.stream(data).min().getAsDouble(), VectorArrayStorageTest.NIL_DELTA);
    }

    /** */
    @Test
    public void maxValue() throws Exception {
        double[] data = initVector();

        Vector.Element maxVal = testVector.maxValue();

        assertEquals(VALUE_NOT_EQUALS, maxVal.get(), Arrays.stream(data).max().getAsDouble(), VectorArrayStorageTest.NIL_DELTA);
    }

    /** */
    @Test
    public void set() throws Exception {
        double[] data = initVector();

        for (int i = 0; i < VectorArrayStorageTest.STORAGE_SIZE; i++)
            testVector.set(i, Math.exp(data[i]));

        for (int i = 0; i < VectorArrayStorageTest.STORAGE_SIZE; i++)
            assertEquals(VALUE_NOT_EQUALS, testVector.get(i), Math.exp(data[i]), VectorArrayStorageTest.NIL_DELTA);
    }

    /** */
    @Test(expected = IndexException.class)
    public void setNegative0() throws Exception {
        testVector.set(-1, -1);
    }

    /** */
    @Test(expected = IndexException.class)
    public void setNegative1() throws Exception {
        initVector();

        testVector.set(-1, -1);
    }

    /** */
    @Test(expected = IndexException.class)
    public void setNegative2() throws Exception {
        initVector();

        testVector.set(VectorArrayStorageTest.STORAGE_SIZE + 1, -1);
    }

    /** */
    @Test
    public void setX() throws Exception {
        double[] data = initVector();

        for (int i = 0; i < VectorArrayStorageTest.STORAGE_SIZE; i++)
            testVector.setX(i, Math.exp(data[i]));

        for (int i = 0; i < VectorArrayStorageTest.STORAGE_SIZE; i++)
            assertEquals(VALUE_NOT_EQUALS, testVector.get(i), Math.exp(data[i]), VectorArrayStorageTest.NIL_DELTA);
    }

    /** */
    @Test(expected = IndexOutOfBoundsException.class)
    public void setXNegative0() throws Exception {
        initVector();

        testVector.setX(-1, -1);
    }

    /** */
    @Test(expected = IndexOutOfBoundsException.class)
    public void setXNegative1() throws Exception {
        initVector();

        testVector.setX(VectorArrayStorageTest.STORAGE_SIZE + 1, -1);
    }

    /** */
    @Test(expected = NullPointerException.class)
    public void setXNegative2() throws Exception {
        testVector.setX(-1, -1);
    }

    /** */
    @Test
    public void increment() throws Exception {
        double[] data = initVector();

        for (int i = 0; i < VectorArrayStorageTest.STORAGE_SIZE; i++)
            testVector.increment(i, 1d);

        for (int i = 0; i < VectorArrayStorageTest.STORAGE_SIZE; i++)
            assertEquals(VALUE_NOT_EQUALS, testVector.get(i), data[i] + 1, VectorArrayStorageTest.NIL_DELTA);
    }

    /** */
    @Test
    public void incrementX() throws Exception {
        double[] data = initVector();

        for (int i = 0; i < VectorArrayStorageTest.STORAGE_SIZE; i++)
            testVector.incrementX(i, 1d);

        for (int i = 0; i < VectorArrayStorageTest.STORAGE_SIZE; i++)
            assertEquals(VALUE_NOT_EQUALS, testVector.get(i), data[i] + 1, VectorArrayStorageTest.NIL_DELTA);
    }

    /** */
    @Test
    public void isZero() throws Exception {
        assertTrue(UNEXPECTED_VALUE, testVector.isZero(0d));
        assertFalse(UNEXPECTED_VALUE, testVector.isZero(1d));
    }

    /** */
    @Test
    public void sum() throws Exception {
        double[] data = initVector();

        assertEquals(VALUE_NOT_EQUALS, testVector.sum(), Arrays.stream(data).sum(), EXPECTED_DELTA);
    }

    /** */ @Test
    public void guid() throws Exception {

    }

    /** */ @Test
    public void equals() throws Exception {

    }

    /** */ @Test
    public void all() throws Exception {

    }

    /** */ @Test
    public void nonZeroElements() throws Exception {

    }

    /** */ @Test
    public void clusterGroup() throws Exception {

    }

    /** */ @Test
    public void foldMap() throws Exception {

    }

    /** */ @Test
    public void foldMap1() throws Exception {

    }

    /** */ @Test
    public void norm() throws Exception {

    }

    /** */ @Test
    public void nonZeroes() throws Exception {

    }

    /** */ @Test
    public void assign() throws Exception {

    }

    /** */ @Test
    public void assign1() throws Exception {

    }

    /** */ @Test
    public void assign2() throws Exception {

    }

    /** */ @Test
    public void assign3() throws Exception {

    }

    /** */ @Test
    public void allSpliterator() throws Exception {

    }

    /** */ @Test
    public void nonZeroSpliterator() throws Exception {

    }

    /** */ @Test
    public void dot() throws Exception {

    }

    /** */ @Test
    public void getLengthSquared() throws Exception {

    }

    /** */ @Test
    public void getDistanceSquared() throws Exception {

    }

    /** */ @Test
    public void dotSelf() throws Exception {

    }

    /** */ @Test
    public void getElement() throws Exception {

    }

    /**
     * create {@link AbstractVector} with storage for tests
     *
     * @param storage {@link VectorStorage}
     * @return AbstractVector
     */
    private AbstractVector getAbstractVector(VectorStorage storage) {
        return new AbstractVector(storage) { // TODO: find out how-to fix this warning
            /** */
            @Override public boolean isDense() {
                return false;
            }

            /** */
            @Override public boolean isSequentialAccess() {
                return false;
            }

            /** */
            @Override public Vector copy() {
                return null;
            }

            /** */
            @Override public Vector divide(double x) {
                return null;
            }

            /** */
            @Override public Vector like(int crd) {
                return null;
            }

            /** */
            @Override public Vector minus(Vector vec) {
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
            @Override public Vector plus(double x) {
                return null;
            }

            /** */
            @Override public Vector plus(Vector vec) {
                return null;
            }

            /** */
            @Override public Vector times(double x) {
                return null;
            }

            /** */
            @Override public Vector times(Vector x) {
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
     * create empty {@link AbstractVector} for tests
     *
     * @return AbstractVector
     */
    private AbstractVector getAbstractVector() {
        return new AbstractVector() { // TODO: find out how-to fix this warning

            /** */
            @Override public boolean isDense() {
                return false;
            }

            /** */
            @Override public boolean isSequentialAccess() {
                return false;
            }

            /** */
            @Override public Vector copy() {
                return null;
            }

            /** */
            @Override public Vector divide(double x) {
                return null;
            }

            /** */
            @Override public Vector like(int crd) {
                return null;
            }

            /** */
            @Override public Vector minus(Vector vec) {
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
            @Override public Vector plus(double x) {
                return null;
            }

            /** */
            @Override public Vector plus(Vector vec) {
                return null;
            }

            /** */
            @Override public Vector times(double x) {
                return null;
            }

            /** */
            @Override public Vector times(Vector x) {
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
     * create {@link VectorStorage} for tests
     *
     * @return VectorStorage
     */
    private VectorStorage createEmptyStorage() {
        return new VectorArrayStorage(VectorArrayStorageTest.STORAGE_SIZE);
    }

    /**
     * create filled {@link VectorStorage} for tests
     *
     * @return VectorStorage
     */
    private VectorStorage createStorage() {
        VectorArrayStorage storage = new VectorArrayStorage(VectorArrayStorageTest.STORAGE_SIZE);

        for (int i = 0; i < VectorArrayStorageTest.STORAGE_SIZE; i++)
            storage.set(i, Math.random());

        return storage;
    }

    /**
     * init vector and return initialized values
     *
     * @return initial values
     */
    private double[] initVector() {
        VectorStorage storage = createStorage();
        double[] data = storage.data().clone();

        testVector.setStorage(storage);
        return data;
    }
}