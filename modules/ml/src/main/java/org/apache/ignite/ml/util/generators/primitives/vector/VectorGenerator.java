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

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.util.generators.DataStreamGenerator;
import org.apache.ignite.ml.util.generators.primitives.scalar.RandomProducer;

/**
 * Basic interface for pseudo-random vectors generators.
 */
public interface VectorGenerator extends Supplier<Vector> {
    /**
     * Maps values of vector generator using mapper.
     *
     * @param mapper Mapper.
     * @return Vector generator with mapped vectors.
     */
    public default VectorGenerator map(IgniteFunction<Vector, Vector> mapper) {
        return () -> mapper.apply(get());
    }

    /**
     * Filters values of vector generator using predicate.
     *
     * @param predicate Predicate.
     * @return Vector generator with filtered vectors.
     */
    public default VectorGenerator filter(IgnitePredicate<Vector> predicate) {
        return () -> {
            Vector v = null;
            do {
                v = get();
            }
            while (!predicate.apply(v));

            return v;
        };
    }

    /**
     * Creates new generator by concatenation of vectors of this generator and other.
     *
     * @param other Other.
     * @return Generator of concatenated vectors.
     */
    public default VectorGenerator concat(VectorGenerator other) {
        return () -> VectorUtils.concat(this.get(), other.get());
    }

    /**
     * Creates new generator by concatenation of vectors of this generator and random producer.
     *
     * @param producer Producer.
     * @return Generator of concatenated vector and noize.
     */
    public default VectorGenerator concat(RandomProducer producer) {
        return () -> VectorUtils.concat(this.get(), VectorUtils.of(producer.get()));
    }

    /**
     * Creates new generator by sum of vectors of this generator and other.
     *
     * @param other Other.
     * @return Generator of vector sums.
     */
    public default VectorGenerator plus(VectorGenerator other) {
        return () -> this.get().plus(other.get());
    }

    /**
     * Creates a permanent rearrangement mapping of features in vector and applies this rearrangement for each vectors
     * of current generator.
     *
     * @return Generator of vectors with shuffled features.
     */
    public default VectorGenerator shuffle() {
        return shuffle(System.currentTimeMillis());
    }

    /**
     * Creates a permanent rearrangement mapping of features in vector and applies this rearrangement for each vectors
     * of current generator.
     *
     * @param seed Seed.
     * @return Generator of vectors with shuffled features.
     */
    public default VectorGenerator shuffle(Long seed) {
        Random rnd = new Random(seed);
        List<Integer> shuffledIds = IntStream.range(0, get().size()).boxed().collect(Collectors.toList());
        Collections.shuffle(shuffledIds, rnd);

        return map(original -> {
            Vector cp = original.copy();
            for (int to = 0; to < cp.size(); to++) {
                int from = shuffledIds.get(to);
                cp.set(to, original.get(from));
            }
            return cp;
        });
    }

    /**
     * Increase vectors of generator by increaseSize and sets to new values random selected feature values from already
     * set components.
     *
     * @param increaseSize Increase size.
     * @return Generator.
     */
    public default VectorGenerator duplicateRandomFeatures(int increaseSize) {
        return duplicateRandomFeatures(increaseSize, System.currentTimeMillis());
    }

    /**
     * Increase vectors of generator by increaseSize and sets to new values random selected feature values from already
     * set components.
     *
     * @param increaseSize Increase size.
     * @param seed Seed.
     * @return Generator.
     */
    public default VectorGenerator duplicateRandomFeatures(int increaseSize, Long seed) {
        A.ensure(increaseSize > 0, "increaseSize > 0");

        Random rnd = new Random(seed);
        return map(original -> {
            double[] values = new double[original.size() + increaseSize];
            for (int i = 0; i < original.size(); i++)
                values[i] = original.get(i);
            for (int i = 0; i < increaseSize; i++) {
                int rndId = rnd.nextInt(original.size());
                values[original.size() + i] = original.get(rndId);
            }
            return VectorUtils.of(values);
        });
    }

    /**
     * Moves all vectors to other position by summing with input vector.
     *
     * @param v Vector.
     * @return Generator with old vectors plus input vector.
     */
    public default VectorGenerator move(Vector v) {
        return map(x -> x.plus(v));
    }

    /**
     * Rotate first two components of all vectors of generator by angle around zero.
     *
     * @param angle Angle.
     * @return Generator.
     */
    public default VectorGenerator rotate(double angle) {
        return rotate(angle, 0, 1);
    }

    /**
     * Rotate selected two components of all vectors of generator by angle around zero.
     *
     * @param angle Angle.
     * @param firstComponent First component id.
     * @param secondComponent Second component id.
     * @return Generator.
     */
    public default VectorGenerator rotate(double angle, int firstComponent, int secondComponent) {
        return map(x -> x.copy()
            .set(firstComponent, x.get(firstComponent) * Math.cos(angle) + x.get(secondComponent) * Math.sin(angle))
            .set(secondComponent, -x.get(firstComponent) * Math.sin(angle) + x.get(secondComponent) * Math.cos(angle))
        );
    }

    /**
     * Adds noize to all components of generated vectors.
     *
     * @param randomProducer Random producer.
     * @return Generator.
     */
    public default VectorGenerator noisify(RandomProducer randomProducer) {
        int vectorSize = get().size();
        return plus(randomProducer.vectorize(vectorSize));
    }

    /**
     * Convert vectors generator to unlabeled data stream generator.
     *
     * @return Data stream generator.
     */
    public default DataStreamGenerator asDataStream() {
        final VectorGenerator gen = this;
        return new DataStreamGenerator() {
            @Override public Stream<LabeledVector<Double>> labeled() {
                return Stream.generate(gen).map(v -> new LabeledVector<>(v, 0.0));
            }
        };
    }
}
