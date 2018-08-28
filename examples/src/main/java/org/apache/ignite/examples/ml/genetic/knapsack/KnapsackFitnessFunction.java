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

package org.apache.ignite.examples.ml.genetic.knapsack;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.ml.genetic.Gene;
import org.apache.ignite.ml.genetic.IFitnessFunction;

/**
 * This example demonstrates how to create a {@link IFitnessFunction}.
 * <p>
 * Your fitness function will vary depending on your particular use case. For this fitness function, we simply want
 * to calculate the weight and value of an individual solution relative to other solutions.</p>
 * <p>
 * To do this, we total the weights and values of all the genes within a chromosome.</p>
 */
public class KnapsackFitnessFunction implements IFitnessFunction {
    /**
     * Calculate fitness.
     *
     * @param genes List of Genes.
     * @return Fitness value.
     */
    public double evaluate(List<Gene> genes) {
        double val = 0;
        double weight = 0;

        List<Long> duplicates = new ArrayList<>();
        int badSolution = 1;

        for (Gene gene : genes) {
            weight = weight + ((Item)(gene.getVal())).getWeight();
            val = val + ((Item)(gene.getVal())).getVal();

            double maximumWeight = 20;
            if (duplicates.contains(gene.id()) || (weight > maximumWeight)) {
                badSolution = 0;
                break;
            }
            else
                duplicates.add(gene.id());
        }

        return (val * badSolution);
    }
}
