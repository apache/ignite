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

package org.apache.ignite.ml.util.generators.primitives.vector;

import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link ParametricVectorGenerator}.
 */
public class ParametricVectorGeneratorTest {
    /** */
    @Test
    public void testGet() {
        Vector vec = new ParametricVectorGenerator(
            () -> 2.,
            t -> t,
            t -> 2 * t,
            t -> 3 * t,
            t -> 100.
        ).get();

        assertEquals(4, vec.size());
        assertArrayEquals(new double[] {2., 4., 6., 100.}, vec.asArray(), 1e-7);
    }

    /** */
    @Test(expected = IllegalArgumentException.class)
    public void testIllegalArguments() {
        new ParametricVectorGenerator(() -> 2.).get();
    }
}
