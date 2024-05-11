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

package org.apache.ignite.ml.util.generators.primitives.scalar;

import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Pseudorandom producer generating values from gauss distribution.
 */
public class GaussRandomProducer extends RandomProducerWithGenerator {
    /** Mean. */
    private final double mean;

    /** Variance. */
    private final double variance;

    /**
     * Creates an instance of GaussRandomProducer with mean = 0 and variance = 1.0.
     */
    public GaussRandomProducer() {
        this(0.0, 1.0, System.currentTimeMillis());
    }

    /**
     * Creates an instance of GaussRandomProducer with mean = 0 and variance = 1.0.
     *
     * @param seed Seed.
     */
    public GaussRandomProducer(long seed) {
        this(0.0, 1.0, seed);
    }

    /**
     * Creates an instance of GaussRandomProducer.
     *
     * @param mean Mean.
     * @param variance Variance.
     */
    public GaussRandomProducer(double mean, double variance) {
        this(mean, variance, System.currentTimeMillis());
    }

    /**
     * Creates an instance of GaussRandomProducer.
     *
     * @param mean Mean.
     * @param variance Variance.
     * @param seed Seed.
     */
    public GaussRandomProducer(double mean, double variance, long seed) {
        super(seed);

        A.ensure(variance > 0, "variance > 0");

        this.mean = mean;
        this.variance = variance;
    }

    /** {@inheritDoc} */
    @Override public Double get() {
        return mean + generator().nextGaussian() * Math.sqrt(variance);
    }
}
