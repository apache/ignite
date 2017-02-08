package org.apache.ignite.math.impls;

import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.math.Functions;
import org.apache.ignite.math.IndexException;
import org.apache.ignite.math.Matrix;
import org.apache.ignite.math.Vector;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.stream.StreamSupport;

import static org.junit.Assert.*;

/**
 * Unit test for {@link AbstractVector}.
 */
public class AbstractVectorTest {
    /** */
    private static final String VALUE_NOT_EQUALS = "Values not equals.";

    /** */
    private static final double SECOND_ARG = 1d;

    /**
     * We assume that we will check calculation precision in other tests.
     */
    private static final double EXPECTED_DELTA = 0.1d;

    /** */
    private static final String UNEXPECTED_VALUE = "Unexpected value.";

    /** */
    private static final String NULL_GUID = "Null GUID.";

    /** */
    private static final String UNEXPECTED_GUID_VALUE = "Unexpected GUID value.";

    /** */
    private static final String EMPTY_GUID = "Empty GUID.";

    /** */
    private static final String VALUES_SHOULD_BE_NOT_EQUALS = "Values should be not equals.";
    public static final String NULL_VALUE = "Null value.";
    public static final String NOT_NULL_VALUE = "Not null value.";
    public static final double TEST_VALUE = 1d;
    public static final String UNSUPPORTED_OPERATION_EXCEPTION_EXPECTED = "UnsupportedOperationException expected.";

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
    public void getX() throws Exception {
        VectorStorage storage = createStorage();

        testVector.setStorage(storage);

        for (int i = 0; i < VectorArrayStorageTest.STORAGE_SIZE; i++)
            assertEquals(VALUE_NOT_EQUALS, testVector.get(i), storage.get(i), VectorArrayStorageTest.NIL_DELTA);
    }

    /** */
    @Test(expected = UnsupportedOperationException.class)
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
            testVector.set(i, Functions.EXP.apply(data[i]));

        for (int i = 0; i < VectorArrayStorageTest.STORAGE_SIZE; i++)
            assertEquals(VALUE_NOT_EQUALS, testVector.get(i), Functions.EXP.apply(data[i]), VectorArrayStorageTest.NIL_DELTA);
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
            testVector.setX(i, Functions.EXP.apply(data[i]));

        for (int i = 0; i < VectorArrayStorageTest.STORAGE_SIZE; i++)
            assertEquals(VALUE_NOT_EQUALS, testVector.get(i), Functions.EXP.apply(data[i]), VectorArrayStorageTest.NIL_DELTA);
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
    @Test(expected = UnsupportedOperationException.class)
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
        assertNotNull(NULL_GUID,testVector.guid());

        assertEquals(UNEXPECTED_GUID_VALUE,testVector.guid(), testVector.guid());

        assertFalse(EMPTY_GUID,testVector.guid().toString().isEmpty());

        testVector = getAbstractVector(createStorage());

        assertNotNull(NULL_GUID,testVector.guid());

        assertEquals(UNEXPECTED_GUID_VALUE,testVector.guid(), testVector.guid());

        assertFalse(EMPTY_GUID,testVector.guid().toString().isEmpty());
    }

    /** */ @Test
    public void equals() throws Exception {
        VectorStorage storage = createStorage();

        AbstractVector testVector1 = getAbstractVector();

        testVector1.setStorage(storage);

        assertTrue(VALUE_NOT_EQUALS, testVector.equals(testVector));

        assertTrue(VALUE_NOT_EQUALS, testVector1.equals(testVector1));

        assertFalse(VALUES_SHOULD_BE_NOT_EQUALS, testVector.equals(testVector1));

        assertFalse(VALUES_SHOULD_BE_NOT_EQUALS, testVector1.equals(testVector));

        testVector1.setStorage(null);

        assertTrue(VALUE_NOT_EQUALS, testVector.equals(testVector1));
    }

    /** */ @Test
    public void all() throws Exception {
        assertNotNull(NULL_VALUE, testVector.all());
        assertNotNull(NULL_VALUE, getAbstractVector(createStorage()).all());
    }

    /** */ @Test
    public void nonZeroElements() throws Exception {
        VectorStorage storage = createStorage();

        assertEquals(VALUE_NOT_EQUALS, testVector.nonZeroElements(), 0);

        testVector.setStorage(storage);

        assertEquals(VALUE_NOT_EQUALS, testVector.nonZeroElements(),Arrays.stream(storage.data()).filter(x -> x!=0d).count());

        addNilValues();

        assertEquals(VALUE_NOT_EQUALS, testVector.nonZeroElements(),Arrays.stream(storage.data()).filter(x -> x!=0d).count());
    }

    /** */ @Test
    public void clusterGroup() throws Exception {

        assertNull(NOT_NULL_VALUE, testVector.clusterGroup());

    }

    /** */ @Test
    public void foldMapWithSecondVector() throws Exception {
        double[] data0 = initVector();

        VectorStorage storage1 = createStorage();

        double[] data1 = storage1.data().clone();

        AbstractVector testVector1 = getAbstractVector(storage1);

        assertEquals(VALUE_NOT_EQUALS, testVector.foldMap(testVector1, Functions.PLUS, Functions.PLUS), Arrays.stream(data0).sum() + Arrays.stream(data1).sum(), EXPECTED_DELTA);

    }

    /** */ @Test
    public void foldMap() throws Exception {
        double[] data = initVector();

        assertEquals(VALUE_NOT_EQUALS, testVector.foldMap(Functions.PLUS, Functions.SIN), Arrays.stream(data).map(Math::sin).sum(), EXPECTED_DELTA);
    }

    /** */ @Test
    public void norm() throws Exception {
        // TODO
    }

    /** */ @Test
    public void nonZeroes() throws Exception {
        assertNotNull(NULL_VALUE, testVector.nonZeroes());

        VectorStorage storage = createStorage();

        double[] data = storage.data();

        testVector.setStorage(storage);

        assertNotNull(NULL_VALUE, testVector.nonZeroes());

        assertEquals(VALUE_NOT_EQUALS, StreamSupport.stream(testVector.nonZeroes().spliterator(), false).count(),Arrays.stream(data).filter(x -> x!=0d).count());

        addNilValues();

        assertEquals(VALUE_NOT_EQUALS, StreamSupport.stream(testVector.nonZeroes().spliterator(), false).count(),Arrays.stream(data).filter(x -> x!=0d).count());

    }

    /** */ @Test (expected = UnsupportedOperationException.class)
    public void assign() throws Exception {
        testVector.assign(TEST_VALUE);
    }

    /** */ @Test
    public void assignPositive(){
        initVector();

        testVector.assign(TEST_VALUE);

        testVector.all().forEach(x -> assertTrue(UNEXPECTED_VALUE, x.get() == TEST_VALUE));
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
     * Create {@link AbstractVector} with storage for tests.
     *
     * @param storage {@link VectorStorage}
     * @return AbstractVector.
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
     * Create empty {@link AbstractVector} for tests.
     *
     * @return AbstractVector.
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
     * Create {@link VectorStorage} for tests.
     *
     * @return VectorStorage
     */
    private VectorStorage createEmptyStorage() {
        return new VectorArrayStorage(VectorArrayStorageTest.STORAGE_SIZE);
    }

    /**
     * Create filled {@link VectorStorage} for tests.
     *
     * @return VectorStorage.
     */
    private VectorStorage createStorage() {
        VectorArrayStorage storage = new VectorArrayStorage(VectorArrayStorageTest.STORAGE_SIZE);

        for (int i = 0; i < VectorArrayStorageTest.STORAGE_SIZE; i++)
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
}