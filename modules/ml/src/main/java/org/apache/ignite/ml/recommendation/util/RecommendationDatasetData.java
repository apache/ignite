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

package org.apache.ignite.ml.recommendation.util;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.recommendation.ObjectSubjectRatingTriplet;
import org.apache.ignite.ml.recommendation.RecommendationTrainer;
import org.apache.ignite.ml.util.Utils;

/**
 * A partition {@code data} of a dataset required in {@link RecommendationTrainer}.
 *
 * @param <O> Type of an object.
 * @param <S> Type of a subject.
 */
public class RecommendationDatasetData<O extends Serializable, S extends Serializable> implements AutoCloseable {
    /** All ratings related to the partition. */
    private final List<? extends ObjectSubjectRatingTriplet<O, S>> ratings;

    /**
     * Constructs a new instance of recommendation dataset data.
     *
     * @param ratings All ratings related to the partition.
     */
    public RecommendationDatasetData(List<? extends ObjectSubjectRatingTriplet<O, S>> ratings) {
        this.ratings = Collections.unmodifiableList(ratings);
    }

    /**
     * Calculates gradient of the loss function of recommendation system SGD training. The details about gradient
     * calculation could be found here: https://tinyurl.com/y6cku9hr.
     *
     * @param objMatrix Object matrix obtained as a result of factorization of rating matrix.
     * @param subjMatrix Subject matrix obtained as a result of factorization of rating matrix.
     * @param batchSize Batch size of stochastic gradient descent. The size of a dataset used on each step of SGD.
     * @param seed Seed (required to make randomized part behaviour repeatable).
     * @param regParam Regularization parameter.
     * @param learningRate Learning rate.
     * @return Gradient of matrix factorization loss function.
     */
    public MatrixFactorizationGradient<O, S> calculateGradient(Map<O, Vector> objMatrix, Map<S, Vector> subjMatrix,
        int batchSize, int seed, double regParam, double learningRate) {
        Map<O, Vector> objGrads = new HashMap<>();
        Map<S, Vector> subjGrads = new HashMap<>();

        int[] rows = getRows(batchSize, seed);
        for (int row : rows) {
            ObjectSubjectRatingTriplet<O, S> triplet = ratings.get(row);
            Vector objVector = objMatrix.get(triplet.getObj());
            Vector subjVector = subjMatrix.get(triplet.getSubj());

            double error = calculateError(objVector, subjVector, triplet.getRating());

            Vector objGrad = (subjVector.times(error).plus(objVector.times(regParam))).times(learningRate);
            Vector subjGrad = (objVector.times(error).plus(subjVector.times(regParam))).times(learningRate);

            objGrads.put(triplet.getObj(), objGrad);
            subjGrads.put(triplet.getSubj(), subjGrad);
        }

        return new MatrixFactorizationGradient<>(objGrads, subjGrads, rows.length);
    }

    /**
     * Returns set of objects contained in {@link #ratings}.
     *
     * @return Set of object.
     */
    public Set<O> getObjects() {
        Set<O> res = new HashSet<>();

        for (ObjectSubjectRatingTriplet<O, S> triplet : ratings)
            res.add(triplet.getObj());

        return res;
    }

    /**
     * Returns set of subjects contained in {@link #ratings}.
     *
     * @return Set of subjects.
     */
    public Set<S> getSubjects() {
        Set<S> res = new HashSet<>();

        for (ObjectSubjectRatingTriplet<O, S> triplet : ratings)
            res.add(triplet.getSubj());

        return res;
    }

    /**
     * Calculates error for the specified object-subject pair.
     *
     * @param wi Vector of {@code W} matrix.
     * @param hi Vector of {@code H} matrix.
     * @param rating Truth rating.
     * @return Error.
     */
    private double calculateError(Vector wi, Vector hi, double rating) {
        if (wi == null || hi == null)
            return rating;

        return wi.dot(hi) - rating;
    }

    /**
     * Returns specified number of distinct rows from {@link #ratings} (indices of rows).
     *
     * @param batchSize Number of rows to be returned.
     * @param seed Seed.
     * @return Specified number of distinct rows from {@link #ratings} (indices of rows).
     */
    private int[] getRows(int batchSize, int seed) {
        return Utils.selectKDistinct(
            ratings.size(),
            Math.min(batchSize, ratings.size()),
            new Random(seed)
        );
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // Do nothing, GC will clean up.
    }
}
