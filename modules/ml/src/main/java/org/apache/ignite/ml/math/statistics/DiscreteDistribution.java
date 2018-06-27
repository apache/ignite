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

package org.apache.ignite.ml.math.statistics;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;
import org.apache.ignite.internal.util.typedef.internal.A;

public class DiscreteDistribution {
    private final TreeMap<Double, Element> distribution;

    public DiscreteDistribution(Element element) {
        this(Collections.singletonList(element));
    }

    public DiscreteDistribution(List<Element> distribution) {
        A.notEmpty(distribution, "distribution");
        this.distribution = new TreeMap<>(Double::compareTo);
        distribution.forEach(e -> this.distribution.put(e.getValue(), e));
    }

    public Double probabilityOf(Double value) {
        Element e = distribution.get(value);
        return e != null ? e.getProbability() : 0.0;
    }

    public Double maximumLikelihoodValue() {
        return distribution.entrySet().stream()
            .max(Comparator.comparing(e -> e.getValue().getProbability()))
            .get().getKey();
    }

    public static class Element {
        private final Double value;
        private final Double probability;

        public Element(Double value, Double probability) {
            this.value = value;
            this.probability = probability;
        }

        public Double getValue() {
            return value;
        }

        public Double getProbability() {
            return probability;
        }
    }
}
