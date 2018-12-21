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

package org.apache.ignite.ml.util.generators.standard;

import java.util.stream.Stream;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.util.generators.DataStreamGenerator;
import org.apache.ignite.ml.util.generators.primitives.scalar.UniformRandomProducer;

/**
 * Represents a generator of regression data stream based on Vector->Double function where each Vector
 * was produced from hypercube with sides = [minXValue, maxXValue].
 */
public class RegressionDataStream implements DataStreamGenerator {
    /** Function. */
    private final IgniteFunction<Vector, Double> function;

    /** Min x value for each dimension. */
    private final double minXValue;

    /** Max x value. */
    private final double maxXValue;

    /** Vector size. */
    private final int vectorSize;

    /** Seed. */
    private long seed;

    /**
     * Creates an instance of RegressionDataStream.
     *
     * @param vectorSize Vector size.
     * @param function Function.
     * @param minXValue Min x value.
     * @param maxXValue Max x value.
     * @param seed Seed.
     */
    private RegressionDataStream(int vectorSize, IgniteFunction<Vector, Double> function,
        double minXValue, double maxXValue, long seed) {

        A.ensure(vectorSize > 0, "vectorSize > 0");
        A.ensure(minXValue <= maxXValue, "minXValue <= maxXValue");

        this.function = function;
        this.minXValue = minXValue;
        this.maxXValue = maxXValue;
        this.seed = seed;
        this.vectorSize = vectorSize;
    }

    /**
     * Creates an instance of RegressionDataStream.
     *
     * @param vectorSize Vector size.
     * @param function Function.
     * @param minXValue Min x value.
     * @param maxXValue Max x value.
     */
    public RegressionDataStream(int vectorSize, IgniteFunction<Vector, Double> function, double minXValue,
        double maxXValue) {
        this(vectorSize, function, minXValue, maxXValue, System.currentTimeMillis());
    }

    /** {@inheritDoc} */
    @Override public Stream<LabeledVector<Vector, Double>> labeled() {
        seed >>= 2;
        return new UniformRandomProducer(minXValue, maxXValue, seed)
            .vectorize(vectorSize).asDataStream()
            .labeled(function);
    }

    /**
     * Creates two dimensional regression data stream.
     *
     * @param function Double->double function.
     * @param minXValue Min x value.
     * @param maxXValue Max x value.
     * @return RegressionDataStream instance.
     */
    public static RegressionDataStream twoDimensional(IgniteFunction<Double, Double> function,
        double minXValue, double maxXValue) {

        return twoDimensional(function, minXValue, maxXValue, System.currentTimeMillis());
    }

    /**
     * Creates two dimensional regression data stream.
     *
     * @param function Double->double function.
     * @param minXValue Min x value.
     * @param maxXValue Max x value.
     * @param seed Seed.
     * @return RegressionDataStream instance.
     */
    public static RegressionDataStream twoDimensional(IgniteFunction<Double, Double> function,
        double minXValue, double maxXValue, long seed) {

        return new RegressionDataStream(1, v -> function.apply(v.get(0)), minXValue, maxXValue, seed);
    }
}
