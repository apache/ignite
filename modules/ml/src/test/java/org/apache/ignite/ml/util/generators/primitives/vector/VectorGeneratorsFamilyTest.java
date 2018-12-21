package org.apache.ignite.ml.util.generators.primitives.vector;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class VectorGeneratorsFamilyTest {
    @Test
    public void testSelection() {
        VectorGeneratorsFamily family = new VectorGeneratorsFamily.Builder()
            .add(() -> VectorUtils.of(1., 2.), 0.5)
            .add(() -> VectorUtils.of(1., 2.), 0.25)
            .add(() -> VectorUtils.of(1., 4.), 0.25)
            .build(0L);

        Map<Integer, Vector> counters = new HashMap<>();
        for (int i = 0; i < 3; i++)
            counters.put(i, VectorUtils.zeroes(2));

        int N = 50000;
        IntStream.range(0, N).forEach(i -> {
            VectorGeneratorsFamily.VectorWithDistributionId vector = family.getWithId();
            int id = vector.distributionId();
            counters.put(id, counters.get(id).plus(vector.vector()));
        });

        for (int i = 0; i < 3; i++)
            counters.put(i, counters.get(i).divide(N));

        assertArrayEquals(new double[]{0.5, 1.0}, counters.get(0).asArray(), 1e-2);
        assertArrayEquals(new double[]{0.25, .5}, counters.get(1).asArray(), 1e-2);
        assertArrayEquals(new double[]{0.25, 1.}, counters.get(2).asArray(), 1e-2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidParameters1() {
        new VectorGeneratorsFamily.Builder().build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidParameters2() {
        new VectorGeneratorsFamily.Builder().add(() -> VectorUtils.of(1.), -1.).build();
    }

    @Test
    public void getWithId() {
    }
}
