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

package org.apache.ignite.ml.util.generators.variable;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;

public interface VectorGenerator extends Supplier<Vector> {
    public static VectorGenerator gauss(double[] pivots, double variance) {
        return gauss(pivots, variance, System.currentTimeMillis());
    }

    public static VectorGenerator gauss(double[] pivots, double variance, long seed) {
        double[] variances = new double[pivots.length];
        Arrays.fill(variances, variance);
        return gauss(pivots, variances, seed);
    }

    public static VectorGenerator gauss(double[] pivots, double[] variances) {
        return gauss(pivots, variances, System.currentTimeMillis());
    }

    public static VectorGenerator gauss(double[] pivots, double[] variances, long seed) {
        GaussRandomProducer[] producers = new GaussRandomProducer[pivots.length];
        for (int i = 0; i < pivots.length; i++) {
            producers[i] = new GaussRandomProducer(pivots[i], variances[i], seed);
            seed >>= 2;
        }

        return vectorize(producers);
    }

    public static VectorGenerator uniform(double min, double max, int numberOfFeatures) {
        return uniform(min, max, numberOfFeatures, System.currentTimeMillis());
    }

    public static VectorGenerator uniform(double min, double max, int numberOfFeatures, long seed) {
        UniformRandomProducer producer = new UniformRandomProducer(min, max, seed);
        return vectorize(numberOfFeatures, producer);
    }

    static VectorGenerator vectorize(RandomProducer[] producers) {
        return () -> {
            double[] values = new double[producers.length];
            for (int i = 0; i < producers.length; i++)
                values[i] = producers[i].get();

            return VectorUtils.of(values);
        };
    }

    static VectorGenerator vectorize(int vecSize, RandomProducer producer) {
        return () -> {
            double[] values = new double[vecSize];
            for (int i = 0; i < vecSize; i++)
                values[i] = producer.get();

            return VectorUtils.of(values);
        };
    }

    static VectorGenerator concat(List<VectorGenerator> generators) {
        return () -> generators.stream().map(Supplier::get).reduce(VectorUtils::concat).get();
    }
}
