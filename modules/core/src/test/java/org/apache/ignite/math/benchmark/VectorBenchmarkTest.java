package org.apache.ignite.math.benchmark;

import java.util.function.Function;
import org.apache.ignite.math.Vector;
import org.apache.ignite.math.impls.DenseLocalOffHeapVector;
import org.apache.ignite.math.impls.DenseLocalOnHeapVector;
import org.junit.Test;

import static org.junit.Assert.*;

/** */
public class VectorBenchmarkTest {
    // todo add benchmarks for map and fold methods (possibly a separate one),
    //  and other methods in Vector and for other types of Vector and Matrix
    /** */ @Test
    public void testDenseLocalOnHeapVector() throws Exception {
        benchmark("DenseLocalOnHeapVector", DenseLocalOnHeapVector::new);
    }

    /** */ @Test
    public void testDenseLocalOffHeapVector() throws Exception {
        benchmark("DenseLocalOffHeapVector", DenseLocalOffHeapVector::new);
    }

    /** */
    private void benchmark(String namePrefix, Function<Integer, Vector> constructor) throws Exception {
        assertNotNull(namePrefix);

        new MathBenchmark(namePrefix + " small sizes").execute(() -> {
            for (int size: new int[] {2, 3, 4, 5, 6, 7}) testMix(size, constructor);
        });

        new MathBenchmark(namePrefix + " sizes powers of 2").execute(() -> {
            for (int power: new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14})
                testMix(1 << power, constructor);
        });

        new MathBenchmark(namePrefix + " large sizes").execute(() -> {
            for (int power: new int[] {10, 12, 14, 16})
                for (int delta : new int[] {-1, 0, 1})
                    testMix((1 << power) + delta, constructor);
        });

        new MathBenchmark(namePrefix + " extra large sizes")
            .measurementTimes(10)
            .execute(() -> { // todo test powers 21, 22, 23 (power 24 killed my IDEA)
            for (int power: new int[] {17, 18, 19, 20})
                for (int delta : new int[] {-1, 0}) // IMPL NOTE delta +1 is not intended for use here
                    testMix((1 << power) + delta, constructor);
        });
    }

    /** */
    private void testMix(int size, Function<Integer, Vector> constructor) {
        final Vector v1 = constructor.apply(size), v2 = constructor.apply(size);

        for (int idx = 0; idx < size; idx++) {
            v1.set(idx, idx);

            v2.set(idx, size - idx);
        }

        assertNotNull(v1.sum());

        assertNotNull(v1.copy());

        assertFalse(v1.getLengthSquared() < 0);

        assertNotNull(v1.normalize());

        assertNotNull(v1.logNormalize());

        assertFalse(v1.getDistanceSquared(v2) < 0);

        assertNotNull(v1.divide(2));

        assertNotNull(v1.minus(v2));

        assertNotNull(v1.plus(v2));

        assertNotNull(v1.dot(v2));

        assertNotNull(v1.assign(v2));

        assertNotNull(v1.assign(1)); // IMPL NOTE this would better be last test for it sets all values the same
    }
}
