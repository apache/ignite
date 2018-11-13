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

package org.apache.ignite.ml.math;

import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link VectorUtils }
 */
public class VectorUtilsTest {
    /** */
    @Test
    public void testOf1() {
        double[] values = {1.0, 2.0, 3.0};
        Vector vector = VectorUtils.of(values);

        assertEquals(3, vector.size());
        assertEquals(3, vector.nonZeroElements());
        for (int i = 0; i < values.length; i++)
            assertEquals(values[i], vector.get(i), 0.001);
    }

    /** */
    @Test
    public void testOf2() {
        Double[] values = {1.0, null, 3.0};
        Vector vector = VectorUtils.of(values);

        assertEquals(3, vector.size());
        assertEquals(2, vector.nonZeroElements());
        for (int i = 0; i < values.length; i++) {
            if (values[i] == null)
                assertEquals(0.0, vector.get(i), 0.001);
            else
                assertEquals(values[i], vector.get(i), 0.001);
        }
    }

    /** */
    @Test(expected = NullPointerException.class)
    public void testFails1() {
        VectorUtils.of((double[])null);
    }

    /** */
    @Test(expected = NullPointerException.class)
    public void testFails2() {
        VectorUtils.of((Double[])null);
    }
}
