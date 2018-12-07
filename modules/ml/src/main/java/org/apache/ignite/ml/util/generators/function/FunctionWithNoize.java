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

package org.apache.ignite.ml.util.generators.function;

import org.apache.ignite.ml.util.generators.variable.RandomProducer;
import org.apache.ignite.ml.math.functions.IgniteFunction;

public class FunctionWithNoize<I> implements RandomFunction<I> {
    private final IgniteFunction<I, Double> baselineFunction;
    private final RandomProducer noize;

    public FunctionWithNoize(IgniteFunction<I, Double> baselineFunction,
        RandomProducer noize) {

        this.baselineFunction = baselineFunction;
        this.noize = noize;
    }

    @Override public Double apply(I x) {
        return baselineFunction.apply(x) + noize.get();
    }
}
