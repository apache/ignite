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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.IntStream;
import org.apache.ignite.math.Vector;
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
    public static final int OFFSET = 10;

    /** */
    public static final int VIEW_LENGHT = 80;

    /** */
    private static final String EXTERNALIZE_TEST_FILE_NAME = "externalizeTest";

    /** */
    VectorView testVector;

    /** */
    DenseLocalOnHeapVector parentVector;

    /** */
    double[] parentData;

    @Before
    public void setup(){
        parentVector = new DenseLocalOnHeapVector(MathTestConstants.STORAGE_SIZE);

        IntStream.range(0, MathTestConstants.STORAGE_SIZE).forEach(idx -> parentVector.set(idx, Math.random()));

        parentData = parentVector.getStorage().data().clone();

        testVector = new VectorView(parentVector, OFFSET, VIEW_LENGHT);
    }

    /** */
    @AfterClass
    public static void cleanup() throws IOException {
        Files.deleteIfExists(Paths.get(EXTERNALIZE_TEST_FILE_NAME));
    }

    /** */
    @Test
    public void copy() throws Exception {
        Vector copy = testVector.copy();

        assertTrue(VALUE_NOT_EQUALS, copy.equals(testVector));
    }

    /** */
    @Test
    public void like() throws Exception {
        // TODO
    }

    /** */
    @Test
    public void likeMatrix() throws Exception {

    }

    /** */
    @Test
    public void writeReadExternal() throws Exception {
        File f = new File(EXTERNALIZE_TEST_FILE_NAME);

        try {
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(f));

            objectOutputStream.writeObject(testVector);

            objectOutputStream.close();

            ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(f));

            VectorView readedVector = (VectorView) objectInputStream.readObject();

            objectInputStream.close();

            assertTrue(VALUE_NOT_EQUALS, testVector.equals(readedVector));
        } catch (ClassNotFoundException | IOException e) {
            fail(e.getMessage());
        }
    }

}