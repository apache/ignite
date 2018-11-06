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

package org.apache.ignite.ml.selection.cv;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents the cross validation procedure result,
 * wraps score and values of hyper parameters associated with these values.
 */
public class CrossValidationResult {
    /** Best hyper params. */
    private Map<String, Double> bestHyperParams;

    /** Best score. */
    private double[] bestScore;

    /**
     * Scoring board.
     * The key is map of hyper parameters and its values,
     * the value is score result associated with set of hyper paramters presented in the key.
     */
    private Map<Map<String, Double>, double[]> scoringBoard = new HashMap<>();

    /**
     * Default constructor.
     */
    CrossValidationResult() {
    }

    /**
     * Gets the best value for the specific hyper parameter.
     *
     * @param hyperParamName Hyper parameter name.
     * @return The value.
     */
    public double getBest(String hyperParamName) {
        return bestHyperParams.get(hyperParamName);
    }

    /**
     * Gets the best score for the specific hyper parameter.
     *
     * @return The value.
     */
    public double[] getBestScore() {
        return bestScore;
    }

    /**
     * Adds local scores and associated parameter set to the scoring board.
     *
     * @param locScores The scores.
     * @param paramMap  The parameter set associated with the given scores.
     */
    void addScores(double[] locScores, Map<String, Double> paramMap) {
        scoringBoard.put(paramMap, locScores);
    }

    /**
     * Gets the the average value of best score array.
     *
     * @return The value.
     */
    public double getBestAvgScore() {
        if (bestScore == null)
            return Double.MIN_VALUE;
        return Arrays.stream(bestScore).average().orElse(Double.MIN_VALUE);
    }

    /**
     * Helper method in cross-validation process.
     *
     * @param bestScore The best score.
     */
    void setBestScore(double[] bestScore) {
        this.bestScore = bestScore;
    }

    /**
     * Helper method in cross-validation process.
     *
     * @param bestHyperParams The best hyper parameters.
     */
    void setBestHyperParams(Map<String, Double> bestHyperParams) {
        this.bestHyperParams = bestHyperParams;
    }

    /**
     * Gets the Scoring Board.
     *
     * The key is map of hyper parameters and its values,
     * the value is score result associated with set of hyper paramters presented in the key.
     *
     * @return The Scoring Board.
     */
    public Map<Map<String, Double>, double[]> getScoringBoard() {
        return scoringBoard;
    }

    /**
     * Gets the best hyper parameters set.
     *
     * @return The value.
     */
    public Map<String, Double> getBestHyperParams() {
        return bestHyperParams;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "CrossValidationResult{" +
            "bestHyperParams=" + bestHyperParams +
            ", bestScore=" + Arrays.toString(bestScore) +
            '}';
    }
}
