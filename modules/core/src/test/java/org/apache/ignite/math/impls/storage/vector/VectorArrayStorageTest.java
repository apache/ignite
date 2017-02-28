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

package org.apache.ignite.math.impls.storage.vector;

import org.junit.Test;

import java.util.Arrays;

import static org.apache.ignite.math.impls.MathTestConstants.*;
import static org.junit.Assert.*;

/**
 * Unit test for {@link VectorArrayStorage}.
 */
public class VectorArrayStorageTest extends VectorBaseStorageTest<VectorArrayStorage> {
    /** */
    @Override public void setUp() {
        storage = new VectorArrayStorage(STORAGE_SIZE);
    }

    /** */
    @Test
    public void isArrayBased() throws Exception {
        assertTrue(WRONG_ATTRIBUTE_VALUE, storage.isArrayBased());

        assertTrue(WRONG_ATTRIBUTE_VALUE, new VectorArrayStorage().isArrayBased());
    }

    /** */
    @Test
    public void data() throws Exception {
        assertNotNull(NULL_DATA_STORAGE, storage.data());

        assertEquals(WRONG_DATA_SIZE, storage.data().length, STORAGE_SIZE);

        assertTrue(UNEXPECTED_DATA_VALUE, Arrays.equals(storage.data(), new double[STORAGE_SIZE]));

        assertNull(UNEXPECTED_DATA_VALUE, new VectorArrayStorage().data());
    }

}
