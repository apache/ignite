/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
