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
 * PasswordFitnessFunction evaluates randomly generated passwords giving higher score to passwords with more special
 * characters.
 */
public class PasswordFitnessFunction implements IFitnessFunction {

    /**
     * @param genes List of Genes within an individual Chromosome
     * @return Fitness score
     */
    @Override
    public double evaluate(List<Gene> genes) {

        double specialCharCount = 0;
        double lowerCaseCount = 0;
        double upperCaseCount = 0;

        double fitness = 0;
        double specialCharScore = 2;

        for (int i = 0; i < genes.size(); i++) {

            Character aCharacter = (Character)(genes.get(i).getValue());
            if (Character.isUpperCase(aCharacter.charValue())) {
                upperCaseCount = upperCaseCount + 1;
            }

            else if (Character.isLowerCase(aCharacter.charValue())) {
                lowerCaseCount = lowerCaseCount + 1;
            }
            else {
                specialCharCount = specialCharCount + 1;
            }
        }

        specialCharCount = specialCharScore * specialCharCount;
        fitness = upperCaseCount + lowerCaseCount + specialCharCount;
        return fitness;
    }
}