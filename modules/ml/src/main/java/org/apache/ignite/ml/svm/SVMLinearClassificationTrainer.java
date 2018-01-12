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

package org.apache.ignite.ml.svm;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.ml.Trainer;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.structures.LabeledDataset;
import org.apache.ignite.ml.structures.LabeledVector;
import org.jetbrains.annotations.NotNull;


/**
 * Base class for SVM linear classification trainer with SDCA algorithm.
 */
public class SVMLinearClassificationTrainer implements Trainer<SVMLinearClassificationModel, LabeledDataset> {
    /** Amount of iterations. */
    private int amountOfIterations = 20;

    /** Amount of local SDCA iterations. */
    private int amountOfLocIterations = 50;

    /**
     * Returns model based on data
     *
     * @param data data to build model
     * @return model
     */
    @Override public SVMLinearClassificationModel train(LabeledDataset data) {

        Vector weights = initializeWeightsWithZeros(data.colSize());
        double intercept = 0.0;

        SVMLinearClassificationModel mdl = new SVMLinearClassificationModel(weights, intercept);

        for (int i = 0; i < amountOfIterations; i++) {
            Vector deltaWeights = calculateUpdates(data, mdl.lambda(), weights, intercept); //DEBUG: maybe should be scaled or normalized
            weights = weights.plus(deltaWeights); // change on in-place operation without recreation of vector
        }

        return mdl.withWeights(weights);
    }

    // DEBUG: rename or change signature
    /** */
    @NotNull private Vector initializeWeightsWithZeros(int vectorSize) {
        return new DenseLocalOnHeapVector(vectorSize);
    }

    /** */
    private Vector calculateUpdates(LabeledDataset data, double lambda, Vector weights,
        double intercept) {
        Vector copiedWeights = weights.copy();
        Vector deltaWeights = initializeWeightsWithZeros(weights.size()); // initialize with zeros

        final int amountOfObservation = data.rowSize();

        Vector tmpAlphas = initializeWeightsWithZeros(amountOfObservation);
        Vector deltaAlphas = initializeWeightsWithZeros(amountOfObservation); // initialize with zeros

        for (int i = 0; i < amountOfLocIterations; i++) {
            int randomIdx = ThreadLocalRandom.current().nextInt(amountOfObservation);
            LabeledVector row = (LabeledVector)data.getRow(randomIdx);
            double alpha = tmpAlphas.get(randomIdx);

            Deltas deltas = maximize(row, alpha, copiedWeights, lambda, amountOfObservation);

            copiedWeights = copiedWeights.plus(deltas.deltaWeights); // need in-place operation; creates new vector
            deltaWeights = deltaWeights.plus(deltas.deltaWeights);  // need in-place operation; creates new vector

            tmpAlphas.set(randomIdx, tmpAlphas.get(randomIdx) + deltas.deltaAlpha);
            deltaAlphas.set(randomIdx, deltaAlphas.get(randomIdx) + deltas.deltaAlpha);
        }
        return deltaWeights;
    }

    /** */
    private Deltas maximize(LabeledVector row, double alpha, Vector weights,
        double lambda, int amountOfObservation) {

        double gradient = calcGradient(row, weights, lambda, amountOfObservation);
        double prjGrad = calculateProjectionGradient(alpha, gradient);

        return calcDeltas(row, alpha, prjGrad, weights.size(), lambda, amountOfObservation);

    }

    /** */
    private Deltas calcDeltas(LabeledVector row, double alpha, double gradient, int vectorSize,
        double lambda,
        int amountOfObservation) {
        if(gradient != 0.0) {

            double qii = row.features().dot(row.features());
            double newAlpha = calcNewAlpha(alpha, gradient, qii);

            Vector deltaWeights = row.features()
                .times((Double)row.label()*(newAlpha - alpha)/(lambda * amountOfObservation));
            return new Deltas (newAlpha - alpha, deltaWeights);
        } else
            return new Deltas(0.0, initializeWeightsWithZeros(vectorSize));

    }

    /** */
    private double calcNewAlpha(double alpha, double gradient, double qii) {
        if(qii != 0.0)
            return Math.min(Math.max(alpha - (gradient/qii), 0.0), 1.0);
        else
            return 1.0;
    }

    /** */
    private double calcGradient(LabeledVector row, Vector weights, double lambda,
        int amountOfObservation) {
        double dotProduct = row.features().dot(weights);
        return ((Double)row.label() * dotProduct - 1.0) * (lambda * amountOfObservation);
    }

    /** */
    private double calculateProjectionGradient(double alpha, double gradient) {
        if(alpha <= 0.0) return Math.min(gradient, 0.0);

        else if (alpha >= 1.0) return Math.max(gradient, 0.0);

        else return gradient;
    }
}

/** */
class Deltas {
    /** */
    public double deltaAlpha;

    /** */
    public Vector deltaWeights;

    public Deltas(double deltaAlpha, Vector deltaWeights) {
        this.deltaAlpha = deltaAlpha;
        this.deltaWeights = deltaWeights;
    }
}


