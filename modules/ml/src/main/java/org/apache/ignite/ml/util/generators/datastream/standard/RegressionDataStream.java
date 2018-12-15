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

package org.apache.ignite.ml.util.generators.datastream.standard;

import java.util.stream.Stream;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.util.generators.datastream.DataStreamGenerator;
import org.apache.ignite.ml.util.generators.primitives.vector.VectorGenerator;

public class RegressionDataStream implements DataStreamGenerator {
    private final IgniteFunction<Vector, Double> function;
    private final double minXValue;
    private final double maxXValue;
    private final int vectorSize;
    private long seed;

    private RegressionDataStream(int vectorSize, IgniteFunction<Vector, Double> function,
        double minXValue, double maxXValue, long seed) {

        this.function = function;
        this.minXValue = minXValue;
        this.maxXValue = maxXValue;
        this.seed = seed;
        this.vectorSize = vectorSize;
    }

    public RegressionDataStream(int vectorSize, IgniteFunction<Vector, Double> function, double minXValue, double maxXValue) {
        this(vectorSize, function, minXValue, maxXValue, System.currentTimeMillis());
    }

    @Override public Stream<LabeledVector<Vector, Double>> labeled() {
        seed >>= 2;
        return VectorGenerator.uniform(minXValue, maxXValue, vectorSize, seed).labeled()
            .map(v -> {
                return new LabeledVector<>(v.features(), function.apply(v.features()));
            });
    }

    public static RegressionDataStream twoDimensional(IgniteFunction<Double, Double> function,
        double minXValue, double maxXValue) {

        return twoDimensional(function, minXValue, maxXValue, System.currentTimeMillis());
    }

    public static RegressionDataStream twoDimensional(IgniteFunction<Double, Double> function,
        double minXValue, double maxXValue, long seed) {

        return new RegressionDataStream(1, v -> function.apply(v.get(0)), minXValue, maxXValue, seed);
    }
}
