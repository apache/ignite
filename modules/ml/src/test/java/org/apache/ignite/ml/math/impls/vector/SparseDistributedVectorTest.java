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

package org.apache.ignite.ml.math.impls.vector;

import org.apache.ignite.Ignite;

import org.apache.ignite.internal.util.IgniteUtils;

import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.Vector;

import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.ml.math.impls.MathTestConstants;

import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Ignore;

import java.io.*;


import static org.apache.ignite.ml.math.impls.MathTestConstants.UNEXPECTED_VAL;

/**
 * Tests for {@link SparseDistributedVector}.
 */
@GridCommonTest(group = "Distributed Models")
public class SparseDistributedVectorTest extends GridCommonAbstractTest {
    /** Number of nodes in grid */
    private static final int NODE_COUNT = 3;
    /** Precision. */
    private static final double PRECISION = 0.0;
    /** */
    private static final int VECTOR_SIZE = 10;
    /** Grid instance. */
    private Ignite ignite;
    /** Vector size */
    private final int size = MathTestConstants.STORAGE_SIZE;
    /** Vector for tests */
    private SparseDistributedVector sparseDistributedVector;

    /**
     * Default constructor.
     */
    public SparseDistributedVectorTest() {
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
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        ignite = grid(NODE_COUNT);

        ignite.configuration().setPeerClassLoadingEnabled(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        if (sparseDistributedVector != null) {
            sparseDistributedVector.destroy();
            sparseDistributedVector = null;
        }
    }

    /** */
    public void testGetSet() throws Exception {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        sparseDistributedVector = new SparseDistributedVector(size, StorageConstants.RANDOM_ACCESS_MODE);

        for (int i = 0; i < size; i++) {
                double v = Math.random();
                sparseDistributedVector.set(i, v);
                assertEquals("Unexpected value for vector element[" + i + "]", v, sparseDistributedVector.get(i), PRECISION);
        }
    }

    /** */
    public void testExternalize() throws IOException, ClassNotFoundException {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        sparseDistributedVector = new SparseDistributedVector(size, StorageConstants.RANDOM_ACCESS_MODE);

        sparseDistributedVector.set(1, 1.0);

        ByteArrayOutputStream byteArrOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objOutputStream = new ObjectOutputStream(byteArrOutputStream);

        objOutputStream.writeObject(sparseDistributedVector);

        ByteArrayInputStream byteArrInputStream = new ByteArrayInputStream(byteArrOutputStream.toByteArray());
        ObjectInputStream objInputStream = new ObjectInputStream(byteArrInputStream);

        SparseDistributedVector objRestored = (SparseDistributedVector)objInputStream.readObject();

        assertTrue(MathTestConstants.VAL_NOT_EQUALS, sparseDistributedVector.equals(objRestored));
        assertEquals(MathTestConstants.VAL_NOT_EQUALS, objRestored.get(1), 1.0, PRECISION);
    }

    /** Test simple math. */
    public void testMath() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        sparseDistributedVector = new SparseDistributedVector(size, StorageConstants.RANDOM_ACCESS_MODE);
        initVector(sparseDistributedVector);

        sparseDistributedVector.assign(2.0);
        for (int i = 0; i < sparseDistributedVector.size(); i++)
                assertEquals(UNEXPECTED_VAL, 2.0, sparseDistributedVector.get(i), PRECISION);

        sparseDistributedVector.plus(3.0);
        for (int i = 0; i < sparseDistributedVector.size(); i++)
                assertEquals(UNEXPECTED_VAL, 5.0, sparseDistributedVector.get(i), PRECISION);

        sparseDistributedVector.times(2.0);
        for (int i = 0; i < sparseDistributedVector.size(); i++)
                assertEquals(UNEXPECTED_VAL, 10.0, sparseDistributedVector.get(i), PRECISION);

        sparseDistributedVector.divide(10.0);
        for (int i = 0; i < sparseDistributedVector.size(); i++)
                assertEquals(UNEXPECTED_VAL, 1.0, sparseDistributedVector.get(i), PRECISION);

       // assertEquals(UNEXPECTED_VAL, sparseDistributedVector.rowSize() * sparseDistributedVector.columnSize(), sparseDistributedVector.sum(), PRECISION);
    }

    /**
     * TODO: IGNITE-5102, wrong min/max, wait for fold/map fix
     */
/*
    @Ignore
    public void testMinMax() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

*/
/*        sparseDistributedVector = new SparseDistributedVector(size, cols, StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);

        for (int i = 0; i < sparseDistributedVector.rowSize(); i++)
            for (int j = 0; j < sparseDistributedVector.columnSize(); j++)
                sparseDistributedVector.set(i, j, i * cols + j + 1);

        assertEquals(UNEXPECTED_VAL, 1.0, sparseDistributedVector.minValue(), PRECISION);
        assertEquals(UNEXPECTED_VAL, size * cols, sparseDistributedVector.maxValue(), PRECISION);

        for (int i = 0; i < sparseDistributedVector.rowSize(); i++)
            for (int j = 0; j < sparseDistributedVector.columnSize(); j++)
                sparseDistributedVector.set(i, j, -1.0 * (i * cols + j + 1));

        assertEquals(UNEXPECTED_VAL, -size * cols, sparseDistributedVector.minValue(), PRECISION);
        assertEquals(UNEXPECTED_VAL, -1.0, sparseDistributedVector.maxValue(), PRECISION);

        for (int i = 0; i < sparseDistributedVector.rowSize(); i++)
            for (int j = 0; j < sparseDistributedVector.columnSize(); j++)
                sparseDistributedVector.set(i, j, i * cols + j);

        assertEquals(UNEXPECTED_VAL, 1.0, sparseDistributedVector.minValue(), PRECISION);
        assertEquals(UNEXPECTED_VAL, size * cols - 1.0, sparseDistributedVector.maxValue(), PRECISION);*//*

    }
*/

    /** */
    public void testMap() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        sparseDistributedVector = new SparseDistributedVector(size, StorageConstants.RANDOM_ACCESS_MODE);
        initVector(sparseDistributedVector);

        sparseDistributedVector.map(i -> 100.0);
        for (int i = 0; i < sparseDistributedVector.size(); i++)
                assertEquals(UNEXPECTED_VAL, 100.0, sparseDistributedVector.get(i), PRECISION);
    }

    /** */
    public void testCopy() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        sparseDistributedVector = new SparseDistributedVector(size, StorageConstants.RANDOM_ACCESS_MODE);

        try {
            sparseDistributedVector.copy();
            fail("UnsupportedOperationException expected.");
        }
        catch (UnsupportedOperationException e) {
            return;
        }
        fail("UnsupportedOperationException expected.");
    }

    /** */
/*    public void testCacheBehaviour() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        SparseDistributedVector cacheVector1 = new SparseDistributedVector(size, StorageConstants.RANDOM_ACCESS_MODE);
        SparseDistributedVector cacheVector2 = new SparseDistributedVector(size, StorageConstants.RANDOM_ACCESS_MODE);

        initVector(cacheVector1);
        initVector(cacheVector2);

        Collection<String> cacheNames = ignite.cacheNames();

        assert cacheNames.contains(((DistributedStorage)cacheVector1.getStorage()).cacheName());

        IgniteCache<RowColMatrixKey, Map<Integer, Double>> cache = ignite.getOrCreateCache(((DistributedStorage)cacheVector1.getStorage()).cacheName());

        Set<Integer> keySet1 = ((SparseDistributedVectorStorage)cacheVector1.getStorage()).getAllKeys();
        Set<Integer> keySet2 = ((SparseDistributedVectorStorage)cacheVector2.getStorage()).getAllKeys();

        assert cache.containsKeys(keySet1) ||
            keySet1.stream().allMatch(k -> cache.invoke(k, (entry, arguments) -> entry.getKey().equals(k) && entry.getValue().size() == 100));
        assert cache.containsKeys(keySet2) ||
            keySet2.stream().allMatch(k -> cache.invoke(k, (entry, arguments) -> entry.getKey().equals(k) && entry.getValue().size() == 100));

        cacheVector2.destroy();

        assert cache.containsKeys(keySet1) ||
            keySet1.stream().allMatch(k -> cache.invoke(k, (entry, arguments) -> entry.getKey().equals(k) && entry.getValue().size() == 100));
        assert !cache.containsKeys(keySet2) &&
            keySet2.stream().allMatch(k -> cache.invoke(k, (entry, arguments) -> entry.getKey().equals(k) && entry.getValue() == null));

        cacheVector1.destroy();

        assert !cache.containsKeys(keySet1) &&
            keySet1.stream().allMatch(k -> cache.invoke(k, (entry, arguments) -> entry.getKey().equals(k) && entry.getValue() == null));
    }*/

    /** */
    public void testLike() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        sparseDistributedVector = new SparseDistributedVector(size, StorageConstants.RANDOM_ACCESS_MODE);

        assertNotNull(sparseDistributedVector.like(1));
    }


/*    *//** *//*
    public void testMatrixTimes(){
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        int size = VECTOR_SIZE;

        SparseDistributedVector cacheMatrix1 = new SparseDistributedVector(size, size, StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);
        SparseDistributedVector cacheMatrix2 = new SparseDistributedVector(size, size, StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);

        for (int i = 0; i < size; i++) {
            cacheMatrix1.setX(i, i, i);
            cacheMatrix2.setX(i, i, i);
        }

        Matrix res = cacheMatrix1.times(cacheMatrix2);

        for(int i = 0; i < size; i++)
            for(int j = 0; j < size; j++)
                if (i == j)
                    assertEquals(UNEXPECTED_VAL, i * i, res.get(i, j), PRECISION);
                else
                    assertEquals(UNEXPECTED_VAL, 0, res.get(i, j), PRECISION);
    }*/

    /** */
    private void initVector(Vector v) {
        for (int i = 0; i < v.size(); i++)
                v.set(i, 1.0);
    }
}
