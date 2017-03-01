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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

import org.apache.ignite.math.Matrix;
import org.apache.ignite.math.Vector;
import org.apache.ignite.math.impls.MathTestConstants;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.math.impls.MathTestConstants.*;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link VectorView}.
 */
public class VectorViewTest {
    /** */
    private static final int OFFSET = 10;

    /** */
    private static final int VIEW_LENGTH = 80;

    /** */
    private static final String EXTERNALIZE_TEST_FILE_NAME = "externalizeTest";

    /** */
    private VectorView testVector;

    /** */
    private DenseLocalOnHeapVector parentVector;

    /** */
    private double[] parentData;

    /** */
    @Before
    public void setup(){
        parentVector = new DenseLocalOnHeapVector(MathTestConstants.STORAGE_SIZE);

        IntStream.range(0, MathTestConstants.STORAGE_SIZE).forEach(idx -> parentVector.set(idx, Math.random()));

        parentData = parentVector.getStorage().data().clone();

        testVector = new VectorView(parentVector, OFFSET, VIEW_LENGTH);
    }

    /** */
    @AfterClass
    public static void cleanup() throws IOException {
        Files.deleteIfExists(Paths.get(EXTERNALIZE_TEST_FILE_NAME));
    }

    /** */
    @Test
    public void testCopy() throws Exception {
        Vector cp = testVector.copy();

        assertTrue(VALUE_NOT_EQUALS, cp.equals(testVector));
    }

    /** */
    @Test
    public void testLike() throws Exception {
        for (int card : new int[] {1, 2, 4, 8, 16, 32, 64, 128})
            consumeSampleVectors((v, desc) -> {
                Vector vLike = new VectorView(v, 0, 0).like(card);

                Class<? extends Vector> expType = v.getClass();

                assertNotNull("Expect non-null like vector for " + expType.getSimpleName() + " in " + desc, vLike);

                assertEquals("Expect size equal to cardinality at " + desc, card, vLike.size());

                Class<? extends Vector> actualType = vLike.getClass();

                assertTrue("Expected matrix type " + expType.getSimpleName()
                        + " should be assignable from actual type " + actualType.getSimpleName() + " in " + desc,
                    expType.isAssignableFrom(actualType));

            });
    }

    /** See also {@link VectorToMatrixTest#testLikeMatrix()}. */
    @Test
    public void testLikeMatrix() throws Exception {
        final Set<String> untested = new LinkedHashSet<>();

        consumeSampleVectors((v, desc) -> {
            final Matrix matrix = new VectorView(v, 0, 0).likeMatrix(1, 1);

            if (matrix == null) {
                untested.add(v.getClass().getSimpleName());

                return;
            }

            Class<? extends Matrix> expMatrixType = v.likeMatrix(1, 1).getClass();

            Class<? extends Matrix> actualMatrixType = matrix.getClass();

            assertTrue("Expected matrix type " + expMatrixType.getSimpleName()
                    + " should be assignable from actual type " + actualMatrixType.getSimpleName() + " in " + desc,
                expMatrixType.isAssignableFrom(actualMatrixType));

            for (int rows : new int[] {0, 1, 2})
                for (int cols : new int[] {0, 1, 2}) {
                    final Matrix actualMatrix = new VectorView(v, 0, 0).likeMatrix(rows, cols);

                    String details = "rows " + rows + " cols " + cols;

                    assertNotNull("Expect non-null matrix for " + details + " in " + desc,
                        actualMatrix);

                    assertEquals("Unexpected number of rows in " + desc, rows, actualMatrix.rowSize());

                    assertEquals("Unexpected number of cols in " + desc, cols, actualMatrix.columnSize());
                }
        });

        if (untested.isEmpty())
            return;

        System.out.println("Method likeMatrix() is not yet ready for testing in VectorViews based on " + untested);
    }

    /** */
    @Test
    public void testWriteReadExternal() throws Exception {
        assertNotNull("Unexpected null parent data", parentData);

        File f = new File(EXTERNALIZE_TEST_FILE_NAME);

        try {
            ObjectOutputStream objOutputStream = new ObjectOutputStream(new FileOutputStream(f));

            objOutputStream.writeObject(testVector);

            objOutputStream.close();

            ObjectInputStream objInputStream = new ObjectInputStream(new FileInputStream(f));

            VectorView readVector = (VectorView) objInputStream.readObject();

            objInputStream.close();

            assertTrue(VALUE_NOT_EQUALS, testVector.equals(readVector));
        } catch (ClassNotFoundException | IOException e) {
            fail(e.getMessage());
        }
    }

    /** */
    private void consumeSampleVectors(BiConsumer<Vector, String> consumer) {
        new VectorImplementationsFixtures().consumeSampleVectors(null, consumer);
    }

}
