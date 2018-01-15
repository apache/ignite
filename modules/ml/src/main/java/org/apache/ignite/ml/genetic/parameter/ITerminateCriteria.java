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

package org.apache.ignite.ml.genetic.parameter;

import org.apache.ignite.ml.genetic.Chromosome;

/**
 * Represents the terminate condition for a genetic algorithm.
 *
 * <p>
 *
 * Implement this interface for your respective use case.
 *
 * </p>
 */
public interface ITerminateCriteria {
    /**
     * @param fittestChromosome Fittest chromosome as of the nth generation
     * @param averageFitnessScore Average fitness score
     * @param generation Current number of generations
     * @return Boolean value to determine when to stop evolution
     */
    public boolean isTerminationConditionMet(Chromosome fittestChromosome, double averageFitnessScore, int generation);
}
