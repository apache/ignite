package org.apache.ignite.ml.util.generators.primitives.vector;

import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ParametricVectorGeneratorTest {
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

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalArguments() {
        new ParametricVectorGenerator(() -> 2.).get();
    }
}
