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

public class UniformRandomProducer extends RandomProducerWithGenerator {
    private final double from;
    private final double to;

    public UniformRandomProducer(double from, double to) {
        this(from, to, System.currentTimeMillis());
    }

    public UniformRandomProducer(double from, double to, long seed) {
        super(seed);

        A.ensure(to >= from, "from > to");

        this.from = from;
        this.to = to;
    }

    @Override public Double get() {
        double result = generator().nextDouble() * (to - from) + from;
        if (result > to)
            result = to;

        return result;
    }
}
