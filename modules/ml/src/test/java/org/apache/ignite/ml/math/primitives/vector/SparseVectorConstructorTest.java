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

package org.apache.ignite.ml.math.primitives.vector;

import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.primitives.vector.impl.SparseVector;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** */
public class SparseVectorConstructorTest {
    /** */
    private static final int IMPOSSIBLE_SIZE = -1;

    /** */
    @Test(expected = AssertionError.class)
    public void negativeSizeTest() {
        assertEquals("Negative size.", IMPOSSIBLE_SIZE,
            new SparseVector(-1, 1).size());
    }

    /** */
    @Test(expected = AssertionError.class)
    public void zeroSizeTest() {
        assertEquals("0 size.", IMPOSSIBLE_SIZE,
            new SparseVector(0, 1).size());
    }

    /** */
    @Test
    public void primitiveTest() {
        assertEquals("1 size, random access.", 1,
            new SparseVector(1, StorageConstants.RANDOM_ACCESS_MODE).size());

        assertEquals("1 size, sequential access.", 1,
            new SparseVector(1, StorageConstants.SEQUENTIAL_ACCESS_MODE).size());

    }
}
