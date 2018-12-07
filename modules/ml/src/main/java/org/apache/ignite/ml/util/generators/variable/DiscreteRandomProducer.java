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

package org.apache.ignite.ml.util.generators.variable;

import java.util.Arrays;
import java.util.stream.Collectors;

public class DiscreteRandomProducer extends RandomProducerWithGenerator {
    private double EPS = 1e-5;
    private final double[] probs;

    public DiscreteRandomProducer(double... probs) {
        this(System.currentTimeMillis(), probs);
    }

    public DiscreteRandomProducer(long seed, double... probs) {
        super(seed);

        if (!checkDistribution(probs))
            throw new IllegalArgumentException(String.format(
                "Illegal distribution prob values [%s]",
                Arrays.stream(probs)
                    .mapToObj(p -> String.format("%.2f", p))
                    .collect(Collectors.joining(",")))
            );

        this.probs = probs;

        Arrays.sort(probs);
        int i = 0;
        int j = probs.length - 1;
        while (i < j) {
            double temp = probs[i];
            probs[i] = probs[j];
            probs[j] = temp;

            i++;
            j--;
        }

        for (i = 1; i < probs.length; i++)
            probs[i] += probs[i - 1];
    }

    @Override public Double get() {
        //TODO: optimize this to binary search.
        double p = generator().nextDouble();
        for(int i = 0; i < probs.length; i++) {
            if(probs[i] > p)
                return (double) i;
        }

        return (double) (probs.length - 1);
    }

    public int size() {
        return probs.length;
    }


    private boolean checkDistribution(double[] probs) {
        boolean moreThanOneElement = probs.length > 1;
        boolean allElementsAreGEZero = Arrays.stream(probs).allMatch(p -> p >= 0.0);
        boolean sumOfProbsEqOne = Math.abs(Arrays.stream(probs).sum() - 1.0) < EPS;

        return allElementsAreGEZero && sumOfProbsEqOne && moreThanOneElement;
    }
}
