package org.apache.ignite.math.impls.vector;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.math.*;
import org.apache.ignite.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.math.functions.*;
import org.apache.ignite.math.impls.*;
import org.apache.ignite.testframework.junits.common.*;
import java.util.stream.*;

/**
 * Tests for {@link CacheVector}.
 */
@GridCommonTest(group = "Distributed Models")
public class CacheVectorTest extends GridCommonAbstractTest {
    /** Number of nodes in grid */
    private static final int NODE_COUNT = 3;
    /** Cache name. */
    private static final String CACHE_NAME = "test-cache";
    /** Grid instance. */
    private Ignite ignite;
    // Default kep mapper.
    private VectorKeyMapper<Integer> keyMapper = new VectorKeyMapper<Integer>() {
        @Override
        public Integer apply(int i) {
            return i;
        }

        @Override
        public boolean isValid(Integer o) {
            return true;
        }
    };

    /**
     * Default constructor.
     */
    public CacheVectorTest(){
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 1; i <= NODE_COUNT; i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     *  {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        ignite = grid(NODE_COUNT);

        ignite.configuration().setPeerClassLoadingEnabled(true);
    }

    /** */
    public void testGetSet() throws Exception {
        final int size = MathTestConstants.STORAGE_SIZE;

        IdentityValueMapper valueMapper = new IdentityValueMapper();
        CacheVector<Integer, Double> cacheVector = new CacheVector<>(size, getCache(), keyMapper, valueMapper);

        for (int i = 0; i < size; i++) {
            double random = Math.random();
            cacheVector.set(i, random);
            assertEquals("Unexpected value.", random, cacheVector.get(i), 0d);
        }
    }

    /** */
    public void testMap(){
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getGridName());

        final int size = MathTestConstants.STORAGE_SIZE;

        IdentityValueMapper valueMapper = new IdentityValueMapper();
        CacheVector<Integer, Double> cacheVector = new CacheVector<>(size, getCache(), keyMapper, valueMapper);

        cacheVector.map(value -> 110d);

        for (int i = 0; i < size; i++)
            assertEquals("Unexpected value.", cacheVector.get(i), 110d, 0d);
    }

    /** */
    public void testMapBiFunc(){
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getGridName());

        final int size = MathTestConstants.STORAGE_SIZE;

        IdentityValueMapper valueMapper = new IdentityValueMapper();
        CacheVector<Integer, Double> cacheVector = new CacheVector<>(size, getCache(), keyMapper, valueMapper);

        cacheVector.map(Functions.PLUS, 1d);

        for (int i = 0; i < size; i++)
            assertEquals("Unexpected value.", cacheVector.get(i), 1d, 0d);
    }

    /** */
    public void testSum(){
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getGridName());

        final int size = MathTestConstants.STORAGE_SIZE;

        IdentityValueMapper valueMapper = new IdentityValueMapper();
        CacheVector<Integer, Double> cacheVector = new CacheVector<>(size, getCache(), keyMapper, valueMapper);

        assert cacheVector.sum() == 0d;
        
        cacheVector.assign(1d);
        
        assertEquals("Unexpected value.", cacheVector.sum(), size, 0d);
    }

    /** */
    public void testAssign(){
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getGridName());

        final int size = MathTestConstants.STORAGE_SIZE;

        IdentityValueMapper valueMapper = new IdentityValueMapper();
        CacheVector<Integer, Double> cacheVector = new CacheVector<>(size, getCache(), keyMapper, valueMapper);

        cacheVector.assign(1d);

        for (int i = 0; i < size; i++)
            assertEquals("Unexpected value.", cacheVector.get(i), 1d, 0d);
    }

    /** */
    public void testAssignRange(){
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getGridName());

        final int size = MathTestConstants.STORAGE_SIZE;

        IdentityValueMapper valueMapper = new IdentityValueMapper();
        CacheVector<Integer, Double> cacheVector = new CacheVector<>(size, getCache(), keyMapper, valueMapper);

        cacheVector.assign(IntStream.range(0, size).asDoubleStream().toArray());

        for (int i = 0; i < size; i++)
            assertEquals("Unexpected value.", cacheVector.get(i), i, 0d);
    }

    /** */
    public void testAssignVector(){
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getGridName());

        final int size = MathTestConstants.STORAGE_SIZE;

        IdentityValueMapper valueMapper = new IdentityValueMapper();
        CacheVector<Integer, Double> cacheVector = new CacheVector<>(size, getCache(), keyMapper, valueMapper);

        Vector testVec = new DenseLocalOnHeapVector(IntStream.range(0, size).asDoubleStream().toArray());

        cacheVector.assign(testVec);

        for (int i = 0; i < size; i++)
            assertEquals("Unexpected value.", cacheVector.get(i), testVec.get(i), 0d);
    }

    /** */
    public void testAssignFunc(){
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getGridName());

        final int size = MathTestConstants.STORAGE_SIZE;

        IdentityValueMapper valueMapper = new IdentityValueMapper();
        CacheVector<Integer, Double> cacheVector = new CacheVector<>(size, getCache(), keyMapper, valueMapper);

        cacheVector.assign(idx -> idx);

        for (int i = 0; i < size; i++)
            assertEquals("Unexpected value.", cacheVector.get(i), i, 0d);
    }

    /** */
    public void testPlus(){
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getGridName());

        final int size = MathTestConstants.STORAGE_SIZE;

        IdentityValueMapper valueMapper = new IdentityValueMapper();
        CacheVector<Integer, Double> cacheVector = new CacheVector<>(size, getCache(), keyMapper, valueMapper);

        cacheVector.plus(1d);

        for (int i = 0; i < size; i++)
            assertEquals("Unexpected value.", cacheVector.get(i), 1d, 0d);
    }

    /** */
    public void testPlusVec(){
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getGridName());

        final int size = MathTestConstants.STORAGE_SIZE;

        IdentityValueMapper valueMapper = new IdentityValueMapper();
        CacheVector<Integer, Double> cacheVector = new CacheVector<>(size, getCache(), keyMapper, valueMapper);

        Vector testVec = new DenseLocalOnHeapVector(IntStream.range(0, size).asDoubleStream().toArray());

        try {
            cacheVector.plus(testVec);
            fail();
        } catch (UnsupportedOperationException e){

        }
    }

    /** */
    public void testDivide(){
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getGridName());

        final int size = MathTestConstants.STORAGE_SIZE;

        IdentityValueMapper valueMapper = new IdentityValueMapper();
        CacheVector<Integer, Double> cacheVector = new CacheVector<>(size, getCache(), keyMapper, valueMapper);

        cacheVector.assign(1d);

        cacheVector.divide(2d);

        for (int i = 0; i < size; i++)
            assertEquals("Unexpected value.", cacheVector.get(i), 1d/2d, 0d);
    }

    /** */
    public void testTimes(){
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getGridName());

        final int size = MathTestConstants.STORAGE_SIZE;

        IdentityValueMapper valueMapper = new IdentityValueMapper();
        CacheVector<Integer, Double> cacheVector = new CacheVector<>(size, getCache(), keyMapper, valueMapper);

        cacheVector.assign(1d);

        cacheVector.times(2d);

        for (int i = 0; i < size; i++)
            assertEquals("Unexpected value.", cacheVector.get(i), 2d, 0d);
    }

    /** */
    public void testTimesVector(){
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getGridName());

        final int size = MathTestConstants.STORAGE_SIZE;

        IdentityValueMapper valueMapper = new IdentityValueMapper();
        CacheVector<Integer, Double> cacheVector = new CacheVector<>(size, getCache(), keyMapper, valueMapper);

        cacheVector.assign(1d);
        Vector testVec = new DenseLocalOnHeapVector(IntStream.range(0, size).asDoubleStream().toArray());

        try {
            cacheVector.times(testVec);
            fail();
        } catch (UnsupportedOperationException e){

        }


    }

    /** */
    public void testMin(){
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getGridName());

        final int size = MathTestConstants.STORAGE_SIZE;

        IdentityValueMapper valueMapper = new IdentityValueMapper();
        CacheVector<Integer, Double> cacheVector = new CacheVector<>(size, getCache(), keyMapper, valueMapper);

        Vector testVec = new DenseLocalOnHeapVector(IntStream.range(0, size).asDoubleStream().toArray());

        cacheVector.assign(testVec);

        assertEquals("Unexpected value.", cacheVector.minValue(), 0d, 0d);
    }

    /** */
    public void testMax(){
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getGridName());

        final int size = MathTestConstants.STORAGE_SIZE;

        IdentityValueMapper valueMapper = new IdentityValueMapper();
        CacheVector<Integer, Double> cacheVector = new CacheVector<>(size, getCache(), keyMapper, valueMapper);

        Vector testVec = new DenseLocalOnHeapVector(IntStream.range(0, size).asDoubleStream().toArray());

        cacheVector.assign(testVec);

        assertEquals("Unexpected value.", cacheVector.maxValue(), testVec.get(size -1), 0d);
    }

    /** */
    public void testLike(){
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getGridName());

        final int size = MathTestConstants.STORAGE_SIZE;

        IdentityValueMapper valueMapper = new IdentityValueMapper();
        CacheVector<Integer, Double> cacheVector = new CacheVector<>(size, getCache(), keyMapper, valueMapper);

        try {
            cacheVector.like(size);
            fail("Unsupported case");
        } catch (UnsupportedOperationException ex){

        }
    }

    /** */
    public void testLikeMatrix(){
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getGridName());

        final int size = MathTestConstants.STORAGE_SIZE;

        IdentityValueMapper valueMapper = new IdentityValueMapper();
        CacheVector<Integer, Double> cacheVector = new CacheVector<>(size, getCache(), keyMapper, valueMapper);

        try {
            cacheVector.likeMatrix(size, size);
            fail("Unsupported case");
        } catch (UnsupportedOperationException ex){

        }
    }

    /** */
    public void testCopy(){
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getGridName());

        final int size = MathTestConstants.STORAGE_SIZE;

        IdentityValueMapper valueMapper = new IdentityValueMapper();
        CacheVector<Integer, Double> cacheVector = new CacheVector<>(size, getCache(), keyMapper, valueMapper);

        try {
            cacheVector.copy();
            fail("Unsupported case");
        } catch (UnsupportedOperationException ex){

        }
    }

    /** */
    private IgniteCache<Integer, Double> getCache() {
        assert ignite != null;

        CacheConfiguration cfg = new CacheConfiguration();
        cfg.setName(CACHE_NAME);

        IgniteCache<Integer, Double> cache = ignite.getOrCreateCache(CACHE_NAME);

        assert cache != null;
        return cache;
    }
}