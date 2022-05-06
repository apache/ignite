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

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Represents the cross validation procedure result,
 * wraps score and values of hyper parameters associated with these values.
 */
public class CrossValidationResult implements Serializable {
    /** Lock for internal state. */
    private final Lock lock = new ReentrantLock();

    /** Best hyper params. */
    private ConcurrentHashMap<String, Double> bestHyperParams;

    /** Best score. */
    private double[] bestScore;

    /**
     * Scoring board.
     * The key is map of hyper parameters and its values,
     * the value is score result associated with set of hyper parameters presented in the key.
     */
    private final Map<ConcurrentHashMap<String, Double>, double[]> scoringBoard = new ConcurrentHashMap<>();

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
        double[] bestScoreCopy;
        lock.lock();
        try {
            bestScoreCopy = bestScore.clone();
        }
        finally {
            lock.unlock();
        }

        return bestScoreCopy;
    }

    /**
     * Adds local scores and associated parameter set to the scoring board.
     *
     * @param locScores The scores.
     * @param paramMap  The parameter set associated with the given scores.
     */
    void addScores(double[] locScores, Map<String, Double> paramMap) {
        lock.lock();
        try {
            double locAvgScore = Arrays.stream(locScores).average().orElse(Double.MIN_VALUE);

            if (locAvgScore >= getBestAvgScore()) {
                bestScore = locScores.clone();
                bestHyperParams = new ConcurrentHashMap<>(paramMap);
            }

            scoringBoard.put(new ConcurrentHashMap<>(paramMap), locScores);
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Gets the the average value of best score array.
     *
     * Default value is Double.MIN_VALUE.
     *
     * @return The value.
     */
    public double getBestAvgScore() {
        lock.lock();
        try {
            if (bestScore == null)
                return Double.MIN_VALUE;
            else
                return Arrays.stream(bestScore).average().orElse(Double.MIN_VALUE);
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Gets the Scoring Board.
     *
     * The key is map of hyper parameters and its values,
     * the value is score result associated with set of hyper parameters presented in the key.
     *
     * @return The Scoring Board.
     */
    public Map<Map<String, Double>, double[]> getScoringBoard() {
        Map<Map<String, Double>, double[]> result;
        lock.lock();
        try {
            result = new HashMap<>();

            scoringBoard.forEach((hyperParams, scores) -> {
                result.put(new HashMap<>(hyperParams), scores.clone());
            });

        }
        finally {
            lock.unlock();
        }

        return result;
    }

    /**
     * Gets the Scoring Board with averaged score.
     *
     * The key is map of hyper parameters and its values,
     * the value is score result associated with set of hyper parameters presented in the key.
     *
     * @return The Scoring Board with averaged score.
     */
    public Map<Map<String, Double>, Double> getScoringBoardWithAverages() {
        Map<Map<String, Double>, Double> result;
        lock.lock();
        try {
            result = new HashMap<>();

            scoringBoard.forEach((hyperParams, scores) -> {
                result.put(new HashMap<>(hyperParams), Arrays.stream(scores).average().orElse(Double.MIN_VALUE));
            });

        }
        finally {
            lock.unlock();
        }

        return result;
    }

    /**
     * Gets the best hyper parameters set.
     *
     * @return The copy of best hyper-parameters map.
     */
    public Map<String, Double> getBestHyperParams() {
        Map<String, Double> result;
        lock.lock();
        try {
            result = new HashMap<>(bestHyperParams);
        }
        finally {
            lock.unlock();
        }

        return result;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "CrossValidationResult{" +
            "bestHyperParams=" + getBestHyperParams() +
            ", bestScore=" + Arrays.toString(getBestScore()) +
            '}';
    }
}
