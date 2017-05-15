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

package org.apache.ignite.ml.math.benchmark;

import java.util.function.BiConsumer;
import java.util.function.Function;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOffHeapVector;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

/** */
public class VectorBenchmarkTest {
    // todo add benchmarks for other methods in Vector and for other types of Vector and Matrix

    /** */
    @Test
    @Ignore("Benchmark tests are intended only for manual execution")
    public void testDenseLocalOnHeapVector() throws Exception {
        benchmark("DenseLocalOnHeapVector basic mix", DenseLocalOnHeapVector::new, this::basicMix);

        benchmark("DenseLocalOnHeapVector fold map", DenseLocalOnHeapVector::new, this::foldMapMix);
    }

    /** */
    @Test
    @Ignore("Benchmark tests are intended only for manual execution")
    public void testDenseLocalOffHeapVector() throws Exception {
        benchmark("DenseLocalOffHeapVector basic mix", DenseLocalOffHeapVector::new, this::basicMix);

        benchmark("DenseLocalOffHeapVector fold map", DenseLocalOffHeapVector::new, this::foldMapMix);
    }

    /** */
    private void benchmark(String namePrefix, Function<Integer, Vector> constructor,
        BiConsumer<Integer, Function<Integer, Vector>> consumer) throws Exception {
        assertNotNull(namePrefix);

        new MathBenchmark(namePrefix + " small sizes").execute(() -> {
            for (int size : new int[] {2, 3, 4, 5, 6, 7})
                consumer.accept(size, constructor);
        });

        new MathBenchmark(namePrefix + " sizes powers of 2").execute(() -> {
            for (int power : new int[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14})
                consumer.accept(1 << power, constructor);
        });

        new MathBenchmark(namePrefix + " large sizes").execute(() -> {
            for (int power : new int[] {10, 12, 14, 16})
                for (int delta : new int[] {-1, 0, 1})
                    consumer.accept((1 << power) + delta, constructor);
        });

        new MathBenchmark(namePrefix + " extra large sizes")
            .measurementTimes(10)
            .execute(() -> { // IMPL NOTE trying below with power 22 almost killed my IDEA and laptop
                for (int power : new int[] {17, 18, 19, 20, 21})
                    for (int delta : new int[] {-1, 0}) // IMPL NOTE delta +1 is not intended for use here
                        consumer.accept((1 << power) + delta, constructor);
            });
    }

    /** */
    private void basicMix(int size, Function<Integer, Vector> constructor) {
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

    /** */
    private void foldMapMix(int size, Function<Integer, Vector> constructor) {
        final Vector v1 = constructor.apply(size), v2 = constructor.apply(size);

        for (int idx = 0; idx < size; idx++) {
            v1.set(idx, idx);

            v2.set(idx, size - idx);
        }

        assertNotNull(v1.map((val) -> (val + 1)));

        assertNotNull(v1.map(v2, (one, other) -> one + other / 2.0));

        assertNotNull(v1.map((val, val1) -> (val + val1), 2.0));

        assertNotNull(v1.foldMap((sum, val) -> (val + sum), (val) -> val, 0.0));

        assertNotNull(v1.foldMap(v2, (sum, val) -> (val + sum), (val1, val2) -> val1 + val2, 0.0));
    }
}
