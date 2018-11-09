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

package org.apache.ignite.examples.ml.genetic.helloworld;

import java.util.List;
import org.apache.ignite.ml.genetic.Gene;
import org.apache.ignite.ml.genetic.IFitnessFunction;

/**
 * This example demonstrates how to create a {@link IFitnessFunction}.
 * <p>
 * Your fitness function will vary depending on your particular use case. For this fitness function, we simply want
 * to calculate the value of an individual solution relative to other solutions.
 * <p>
 * To do this, we increase fitness score by '1' for each character that is in correct position.</p>
 * <p>
 * For our solution, genetic algorithm will continue until we achieve a fitness score of '11', as 'HELLO WORLD'
 * contains 11 characters.</p>
 */
public class HelloWorldFitnessFunction implements IFitnessFunction {
    /**
     * Calculate fitness.
     *
     * @param genes List of Genes.
     * @return Fitness value.
     */
    public double evaluate(List<Gene> genes) {
        double matches = 0;

        for (int i = 0; i < genes.size(); i++) {
            String targetStr = "HELLO WORLD";
            if (genes.get(i).getVal().equals(targetStr.charAt(i)))
                matches = matches + 1;
        }
        return matches;
    }
}
