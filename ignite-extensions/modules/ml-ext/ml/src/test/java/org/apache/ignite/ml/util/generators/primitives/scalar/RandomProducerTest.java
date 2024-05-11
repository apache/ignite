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

package org.apache.ignite.ml.util.generators.primitives.scalar;

import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link RandomProducer}.
 */
public class RandomProducerTest {
    /** */
    @Test
    public void testVectorize() {
        RandomProducer p = () -> 1.0;
        Vector vec = p.vectorize(3).get();

        assertEquals(3, vec.size());
        assertArrayEquals(new double[] {1., 1., 1.}, vec.asArray(), 1e-7);
    }

    /** */
    @Test
    public void testVectorize2() {
        Vector vec = RandomProducer.vectorize(
            () -> 1.0,
            () -> 2.0,
            () -> 3.0
        ).get();

        assertEquals(3, vec.size());
        assertArrayEquals(new double[] {1., 2., 3.}, vec.asArray(), 1e-7);
    }

    /** */
    @Test(expected = IllegalArgumentException.class)
    public void testVectorizeFail() {
        RandomProducer.vectorize();
    }

    /** */
    @Test
    public void testNoizify1() {
        IgniteFunction<Double, Double> f = v -> 2 * v;
        RandomProducer p = () -> 1.0;

        IgniteFunction<Double, Double> res = p.noizify(f);

        for (int i = 0; i < 10; i++)
            assertEquals(2 * i + 1.0, res.apply((double)i), 1e-7);
    }

    /** */
    @Test
    public void testNoizify2() {
        RandomProducer p = () -> 1.0;
        assertArrayEquals(new double[] {1., 2.}, p.noizify(VectorUtils.of(0., 1.)).asArray(), 1e-7);
    }
}
