package org.apache.ignite.ml.math;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

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
        double[] values = null;
        VectorUtils.of(values);
    }

    /** */
    @Test(expected = NullPointerException.class)
    public void testFails2() {
        Double[] values = null;
        VectorUtils.of(values);
    }
}
