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

import org.apache.ignite.ml.math.exceptions.math.CardinalityException;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.util.generators.primitives.scalar.UniformRandomProducer;
import org.junit.Test;
import org.junit.internal.ArrayComparisonFailure;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link VectorGenerator}.
 */
public class VectorGeneratorTest {
    /** */
    @Test
    public void testMap() {
        Vector originalVec = new UniformRandomProducer(-1, 1).vectorize(2).get();
        Vector doubledVec = VectorGeneratorPrimitives.constant(originalVec).map(v -> v.times(2.)).get();
        assertArrayEquals(originalVec.times(2.).asArray(), doubledVec.asArray(), 1e-7);
    }

    /** */
    @Test
    public void testFilter() {
        new UniformRandomProducer(-1, 1).vectorize(2)
            .filter(v -> v.get(0) < 0.5)
            .filter(v -> v.get(1) > -0.5)
            .asDataStream().unlabeled().limit(100)
            .forEach(v -> assertTrue(v.get(0) < 0.5 && v.get(1) > -0.5));
    }

    /** */
    @Test
    public void concat1() {
        VectorGenerator g1 = VectorGeneratorPrimitives.constant(VectorUtils.of(1., 2.));
        VectorGenerator g2 = VectorGeneratorPrimitives.constant(VectorUtils.of(3., 4.));
        VectorGenerator g12 = g1.concat(g2);
        VectorGenerator g21 = g2.concat(g1);

        assertArrayEquals(new double[] {1., 2., 3., 4.}, g12.get().asArray(), 1e-7);
        assertArrayEquals(new double[] {3., 4., 1., 2.}, g21.get().asArray(), 1e-7);
    }

    /** */
    @Test
    public void concat2() {
        VectorGenerator g1 = VectorGeneratorPrimitives.constant(VectorUtils.of(1., 2.));
        VectorGenerator g2 = g1.concat(() -> 1.0);

        assertArrayEquals(new double[] {1., 2., 1.}, g2.get().asArray(), 1e-7);
    }

    /** */
    @Test
    public void plus() {
        VectorGenerator g1 = VectorGeneratorPrimitives.constant(VectorUtils.of(1., 2.));
        VectorGenerator g2 = VectorGeneratorPrimitives.constant(VectorUtils.of(3., 4.));
        VectorGenerator g12 = g1.plus(g2);
        VectorGenerator g21 = g2.plus(g1);

        assertArrayEquals(new double[] {4., 6.}, g21.get().asArray(), 1e-7);
        assertArrayEquals(g21.get().asArray(), g12.get().asArray(), 1e-7);
    }

    /** */
    @Test(expected = CardinalityException.class)
    public void testPlusForDifferentSizes1() {
        VectorGenerator g1 = VectorGeneratorPrimitives.constant(VectorUtils.of(1., 2.));
        VectorGenerator g2 = VectorGeneratorPrimitives.constant(VectorUtils.of(3.));
        g1.plus(g2).get();
    }

    /** */
    @Test(expected = CardinalityException.class)
    public void testPlusForDifferentSizes2() {
        VectorGenerator g1 = VectorGeneratorPrimitives.constant(VectorUtils.of(1., 2.));
        VectorGenerator g2 = VectorGeneratorPrimitives.constant(VectorUtils.of(3.));
        g2.plus(g1).get();
    }

    /** */
    @Test
    public void shuffle() {
        VectorGenerator g1 = VectorGeneratorPrimitives.constant(VectorUtils.of(1., 2., 3., 4.))
            .shuffle(0L);

        double[] exp = {4., 1., 2., 3.};
        Vector v1 = g1.get();
        Vector v2 = g1.get();
        assertArrayEquals(exp, v1.asArray(), 1e-7);
        assertArrayEquals(v1.asArray(), v2.asArray(), 1e-7);
    }

    /** */
    @Test
    public void duplicateRandomFeatures() {
        VectorGenerator g1 = VectorGeneratorPrimitives.constant(VectorUtils.of(1., 2., 3., 4.))
            .duplicateRandomFeatures(2, 1L);

        double[] exp = {1., 2., 3., 4., 3., 1.};
        Vector v1 = g1.get();
        Vector v2 = g1.get();

        assertArrayEquals(exp, v1.asArray(), 1e-7);

        try {
            assertArrayEquals(v1.asArray(), v2.asArray(), 1e-7);
        }
        catch (ArrayComparisonFailure e) {
            //this is valid situation - duplicator should get different features
        }
    }

    /** */
    @Test(expected = IllegalArgumentException.class)
    public void testWithNegativeIncreaseSize() {
        VectorGeneratorPrimitives.constant(VectorUtils.of(1., 2., 3., 4.))
            .duplicateRandomFeatures(-2, 1L).get();
    }

    /** */
    @Test
    public void move() {
        Vector res = VectorGeneratorPrimitives.constant(VectorUtils.of(1., 1.))
            .move(VectorUtils.of(2., 4.))
            .get();

        assertArrayEquals(new double[] {3., 5.}, res.asArray(), 1e-7);
    }

    /** */
    @Test(expected = CardinalityException.class)
    public void testMoveWithDifferentSizes1() {
        VectorGeneratorPrimitives.constant(VectorUtils.of(1., 1.))
            .move(VectorUtils.of(2.))
            .get();
    }

    /** */
    @Test(expected = CardinalityException.class)
    public void testMoveWithDifferentSizes2() {
        VectorGeneratorPrimitives.constant(VectorUtils.of(1.))
            .move(VectorUtils.of(2., 1.))
            .get();
    }

    /** */
    @Test
    public void rotate() {
        double[] angles = {0., Math.PI / 2, -Math.PI / 2, Math.PI, 2 * Math.PI, Math.PI / 4};
        Vector[] exp = new Vector[] {
            VectorUtils.of(1., 0., 100.),
            VectorUtils.of(0., -1., 100.),
            VectorUtils.of(0., 1., 100.),
            VectorUtils.of(-1., 0., 100.),
            VectorUtils.of(1., 0., 100.),
            VectorUtils.of(0.707, -0.707, 100.)
        };

        for (int i = 0; i < angles.length; i++) {
            Vector res = VectorGeneratorPrimitives.constant(VectorUtils.of(1., 0., 100.))
                .rotate(angles[i]).get();
            assertArrayEquals(exp[i].asArray(), res.asArray(), 1e-3);
        }
    }

    /** */
    @Test
    public void noisify() {
        Vector res = VectorGeneratorPrimitives.constant(VectorUtils.of(1., 0.))
            .noisify(() -> 0.5).get();
        assertArrayEquals(new double[] {1.5, 0.5}, res.asArray(), 1e-7);
    }
}
