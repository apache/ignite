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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.util.generators.DataStreamGenerator;

public interface VectorGenerator extends Supplier<Vector>, DataStreamGenerator {
    public default VectorGenerator concat(VectorGenerator other) {
        return () -> VectorUtils.concat(this.get(), other.get());
    }

    public default VectorGenerator shuffle() {
        return shuffle(System.currentTimeMillis());
    }

    public default VectorGenerator shuffle(Long seed) {
        Random rnd = new Random(seed);
        List<Integer> shuffledIds = IntStream.range(0, get().size()).boxed().collect(Collectors.toList());
        Collections.shuffle(shuffledIds, rnd);

        final Map<Integer, Integer> shuffleMap = new HashMap<>();
        for(int i = 0; i < shuffledIds.size(); i++)
            shuffleMap.put(i, shuffledIds.get(i));

        return () -> {
            Vector original = get();
            Vector copy = original.copy();
            for(int to = 0; to < copy.size(); to++) {
                int from = shuffleMap.get(to);
                copy.set(to, original.get(from));
            }
            return copy;
        };
    }

    public default VectorGenerator duplicateRandomFeatures(int increaseSize) {
        return duplicateRandomFeatures(increaseSize, System.currentTimeMillis());
    }

    public default VectorGenerator duplicateRandomFeatures(int increaseSize, Long seed) {
        Random rnd = new Random(seed);
        Vector v = get();
        int[] featuresDuplicateIds = rnd.ints().limit(increaseSize).map(i -> i % v.size()).toArray();
        return () -> {
            Vector original = get();
            double[] values = new double[original.size() + increaseSize];
            for(int i = 0; i < original.size(); i++)
                values[i] = original.get(i);
            for(int i = 0; i < featuresDuplicateIds.length; i++)
                values[original.size() + i] = original.get(featuresDuplicateIds[i]);
            return VectorUtils.of(values);
        };
    }

    @Override
    public default Stream<Vector> unlabeled() {
        return Stream.generate(this);
    }

    @Override
    public default Stream<LabeledVector<Vector, Double>> labeled() {
        return labeled(x -> 0.0);
    }
}
