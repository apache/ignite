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

package org.apache.ignite.ml.trees.trainers.columnbased.regcalcs;

import it.unimi.dsi.fastutil.doubles.Double2IntOpenHashMap;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.trees.trainers.columnbased.ColumnDecisionTreeTrainerInput;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.stream.DoubleStream;

/** Some commonly used functions for calculations of regions of space which correspond to decision tree leaf nodes. */
public class RegionCalculators {
    /** Mean value in the region. */
    public static final IgniteFunction<DoubleStream, Double> MEAN = s -> s.average().orElse(0.0);

    /** Most common value in the region. */
    public static final IgniteFunction<DoubleStream, Double> MOST_COMMON =
        s -> {
            PrimitiveIterator.OfDouble itr = s.iterator();
            Map<Double, Integer> voc = new HashMap<>();

            while (itr.hasNext())
                voc.compute(itr.next(), (d, i) -> i != null ? i + 1 : 0);

            return voc.entrySet().stream().max(Comparator.comparing(Map.Entry::getValue)).map(Map.Entry::getKey).orElse(0.0);
        };

    /** Variance of a region. */
    public static final IgniteFunction<ColumnDecisionTreeTrainerInput, IgniteFunction<DoubleStream, Double>> VARIANCE = input ->
        s -> {
            PrimitiveIterator.OfDouble itr = s.iterator();
            int i = 0;

            double mean = 0.0;
            double m2 = 0.0;

            while (itr.hasNext()) {
                i++;
                double x = itr.next();
                double delta = x - mean;
                mean += delta / i;
                double delta2 = x - mean;
                m2 += delta * delta2;
            }

            return i > 0 ? m2 / i : 0.0;
        };

    /** Gini impurity of a region. */
    public static final IgniteFunction<ColumnDecisionTreeTrainerInput, IgniteFunction<DoubleStream, Double>> GINI = input ->
        s -> {
            PrimitiveIterator.OfDouble itr = s.iterator();

            Double2IntOpenHashMap m = new Double2IntOpenHashMap();

            int size = 0;

            while (itr.hasNext()) {
                size++;
                m.compute(itr.next(), (a, i) -> i != null ? i + 1 : 1);
            }

            double c2 = m.values().stream().mapToDouble(v -> v * v).sum();

            return size != 0 ? 1 - c2 / (size * size) : 0.0;
        };
}
