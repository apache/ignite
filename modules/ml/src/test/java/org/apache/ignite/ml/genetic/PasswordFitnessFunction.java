/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
    @Override public double evaluate(List<Gene> genes) {

        double specialCCnt = 0;
        double lowerCaseCnt = 0;
        double upperCaseCnt = 0;

        double fitness;
        double specialCScore = 2;

        for (Gene gene : genes) {
            Character aCharacter = (Character)(gene.getVal());
            if (Character.isUpperCase(aCharacter))
                upperCaseCnt = upperCaseCnt + 1;

            else if (Character.isLowerCase(aCharacter))
                lowerCaseCnt = lowerCaseCnt + 1;
            else
                specialCCnt = specialCCnt + 1;
        }

        specialCCnt = specialCScore * specialCCnt;
        fitness = upperCaseCnt + lowerCaseCnt + specialCCnt;
        return fitness;
    }
}
