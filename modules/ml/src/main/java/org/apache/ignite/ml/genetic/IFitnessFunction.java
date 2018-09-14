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

package org.apache.ignite.ml.genetic;

import java.util.List;

/**
 * Fitness function are used to determine how optimal a particular solution is relative to other solutions.
 *
 * <p> The evaluate() method should be implemented for this interface. The fitness function is provided list of Genes.
 *
 * The evaluate method should return a positive double value that reflects fitness score.
 *
 * </p>
 */
public interface IFitnessFunction {
    /**
     * @param genes Genes within an individual Chromosome
     * @return Fitness score
     */
    public double evaluate(List<Gene> genes);
}
