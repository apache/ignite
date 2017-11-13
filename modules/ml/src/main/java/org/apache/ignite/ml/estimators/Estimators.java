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

package org.apache.ignite.ml.estimators;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;

/** Estimators. */
public class Estimators {
    /** Simple implementation of mean squared error estimator. */
    public static <T, V> IgniteTriFunction<Model<T, V>, Stream<IgniteBiTuple<T, V>>, Function<V, Double>, Double> MSE() {
        return (model, stream, f) -> stream.mapToDouble(dp -> {
            double diff = f.apply(dp.get2()) - f.apply(model.predict(dp.get1()));
            return diff * diff;
        }).average().orElse(0);
    }

    /** Simple implementation of errors percentage estimator. */
    public static <T, V> IgniteTriFunction<Model<T, V>, Stream<IgniteBiTuple<T, V>>, Function<V, Double>, Double> errorsPercentage() {
        return (model, stream, f) -> {
            AtomicLong total = new AtomicLong(0);

            long cnt = stream.
                peek((ib) -> total.incrementAndGet()).
                filter(dp -> !model.predict(dp.get1()).equals(dp.get2())).
                count();

            return (double)cnt / total.get();
        };
    }
}
