package org.apache.ignite.ml.util.generators.primitives.vector;

import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Test;

import static org.junit.Assert.*;

public class VectorGeneratorPrimitivesTest {
    @Test
    public void testConstant() {
        Vector vec = VectorUtils.of(1.0, 0.0);
        assertArrayEquals(vec.copy().asArray(), VectorGeneratorPrimitives.constant(vec).get().asArray(), 1e-7);
    }

    @Test
    public void testZero() {
        assertArrayEquals(new double[]{0., 0.}, VectorGeneratorPrimitives.zero(2).get().asArray(), 1e-7);
    }
}
