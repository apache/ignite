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

import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.util.generators.primitives.scalar.GaussRandomProducer;
import org.apache.ignite.ml.util.generators.primitives.scalar.RandomProducer;
import org.apache.ignite.ml.util.generators.primitives.scalar.UniformRandomProducer;

/**
 * Collection of predefined vector generators.
 */
public class VectorGeneratorPrimitives {
    /**
     * Returns vector generator of vectors from multidimensional gauss distribution.
     *
     * @param means Mean values per dimension.
     * @param variances Variance values per dimension.
     * @param seed Seed.
     * @return Generator.
     */
    public static VectorGenerator gauss(Vector means, Vector variances, Long seed) {
        A.notEmpty(means.asArray(), "mean.size() != 0");
        A.ensure(means.size() == variances.size(), "mean.size() == variances.size()");

        RandomProducer[] producers = new RandomProducer[means.size()];
        for (int i = 0; i < producers.length; i++)
            producers[i] = new GaussRandomProducer(means.get(i), variances.get(i), seed *= 2);
        return RandomProducer.vectorize(producers);
    }

    /**
     * Returns vector generator of vectors from multidimensional gauss distribution.
     *
     * @param means Mean values per dimension.
     * @param variances Variance values per dimension.
     * @return Generator.
     */
    public static VectorGenerator gauss(Vector means, Vector variances) {
        return gauss(means, variances, System.currentTimeMillis());
    }

    /**
     * Returns vector generator of 2D-vectors from ring-like distribution.
     *
     * @param radius Ring radius.
     * @param fromAngle From angle.
     * @param toAngle To angle.
     * @return Generator.
     */
    public static VectorGenerator ring(double radius, double fromAngle, double toAngle) {
        return ring(radius, fromAngle, toAngle, System.currentTimeMillis());
    }

    /**
     * Returns vector generator of 2D-vectors from ring-like distribution around zero.
     *
     * @param radius Ring radius.
     * @param fromAngle From angle.
     * @param toAngle To angle.
     * @param seed Seed.
     * @return Generator.
     */
    public static VectorGenerator ring(double radius, double fromAngle, double toAngle, long seed) {
        return new ParametricVectorGenerator(
            new UniformRandomProducer(fromAngle, toAngle, seed),
            t -> radius * Math.sin(t),
            t -> radius * Math.cos(t)
        );
    }

    /**
     * Returns vector generator of vectors from multidimensional uniform distribution around zero.
     *
     * @param bounds Parallelogram bounds.
     * @return Generator.
     */
    public static VectorGenerator parallelogram(Vector bounds) {
        return parallelogram(bounds, System.currentTimeMillis());
    }

    /**
     * Returns vector generator of vectors from multidimensional uniform distribution around zero.
     *
     * @param bounds Parallelogram bounds.
     * @param seed Seed.
     * @return Generator.
     */
    public static VectorGenerator parallelogram(Vector bounds, long seed) {
        A.ensure(bounds.size() != 0, "bounds.size() != 0");

        UniformRandomProducer[] producers = new UniformRandomProducer[bounds.size()];
        for (int i = 0; i < producers.length; i++)
            producers[i] = new UniformRandomProducer(-bounds.get(i), bounds.get(i), seed *= 2);

        return RandomProducer.vectorize(producers);
    }

    /**
     * Returns vector generator of 2D-vectors from circle-like distribution around zero.
     *
     * @param radius Circle radius.
     * @return Generator.
     */
    public static VectorGenerator circle(double radius) {
        return circle(radius, System.currentTimeMillis());
    }

    /**
     * Returns vector generator of 2D-vectors from circle-like distribution around zero.
     *
     * @param radius Circle radius.
     * @param seed Seed.
     * @return Generator.
     */
    public static VectorGenerator circle(double radius, long seed) {
        return new UniformRandomProducer(-radius, radius, seed)
            .vectorize(2)
            .filter(v -> Math.sqrt(v.getLengthSquared()) <= radius);
    }

    /**
     * @param size Vector size.
     * @return Generator of constant vector = zero.
     */
    public static VectorGenerator zero(int size) {
        return constant(VectorUtils.zeroes(size));
    }

    /**
     * @param v Constant.
     * @return Generator of constant vector.
     */
    public static VectorGenerator constant(Vector v) {
        return () -> v;
    }
}
