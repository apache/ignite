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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.MathTestConstants;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

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

        Vector cp = sparseDistributedVector.copy();

        assertNotNull(cp);

        for (int i = 0; i < size; i++)
            assertEquals(UNEXPECTED_VAL, cp.get(i), sparseDistributedVector.get(i), PRECISION);
    }


    /** */
    public void testLike() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        sparseDistributedVector = new SparseDistributedVector(size, StorageConstants.RANDOM_ACCESS_MODE);

        assertNotNull(sparseDistributedVector.like(1));
    }


    /** */
    private void initVector(Vector v) {
        for (int i = 0; i < v.size(); i++)
            v.set(i, 1.0);
    }
}