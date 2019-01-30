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
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.util.generators.DataStreamGenerator;
import org.apache.ignite.ml.util.generators.primitives.scalar.UniformRandomProducer;

/**
 * 2D-Vectors data stream with two separable classes.
 */
public class TwoSeparableClassesDataStream implements DataStreamGenerator {
    /** Margin. */
    private final double margin;

    /** Variance. */
    private final double variance;

    /** Seed. */
    private long seed;

    /**
     * Create an instance of TwoSeparableClassesDataStream. Note that margin can be less than zero.
     *
     * @param margin Margin.
     * @param variance Variance.
     */
    public TwoSeparableClassesDataStream(double margin, double variance) {
        this(margin, variance, System.currentTimeMillis());
    }

    /**
     * Create an instance of TwoSeparableClassesDataStream. Note that margin can be less than zero.
     *
     * @param margin Margin.
     * @param variance Variance.
     * @param seed Seed.
     */
    public TwoSeparableClassesDataStream(double margin, double variance, long seed) {
        this.margin = margin;
        this.variance = variance;
        this.seed = seed;
    }

    /** {@inheritDoc} */
    @Override public Stream<LabeledVector<Double>> labeled() {
        seed *= 2;

        double minCordVal = -variance - Math.abs(margin);
        double maxCordVal = variance + Math.abs(margin);

        return new UniformRandomProducer(minCordVal, maxCordVal, seed)
            .vectorize(2).asDataStream().labeled(this::classify)
            .map(v -> new LabeledVector<>(applyMargin(v.features()), v.label()))
            .filter(v -> between(v.features().get(0), -variance, variance))
            .filter(v -> between(v.features().get(1), -variance, variance));
    }

    /** */
    private boolean between(double x, double min, double max) {
        return x >= min && x <= max;
    }

    /** */
    private double classify(Vector v) {
        return v.get(0) - v.get(1) > 0 ? -1.0 : 1.0;
    }

    /** */
    private Vector applyMargin(Vector v) {
        Vector cp = v.copy();

        cp.set(0, cp.get(0) + Math.signum(v.get(0) - v.get(1)) * margin);
        cp.set(1, cp.get(1) - Math.signum(v.get(0) - v.get(1)) * margin);

        return cp;
    }
}
