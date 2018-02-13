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
import org.apache.ignite.ml.math.impls.vector.SparseDistributedVector;
import org.apache.ignite.ml.structures.LabeledDataset;
import org.apache.ignite.ml.structures.LabeledVector;
import org.jetbrains.annotations.NotNull;

/**
 * Base class for a soft-margin SVM linear classification trainer based on the communication-efficient distributed dual
 * coordinate ascent algorithm (CoCoA) with hinge-loss function. <p> This trainer takes input as Labeled Dataset with -1
 * and +1 labels for two classes and makes binary classification. </p> The paper about this algorithm could be found
 * here https://arxiv.org/abs/1409.1458.
 */
public class SVMLinearBinaryClassificationTrainer implements Trainer<SVMLinearBinaryClassificationModel, LabeledDataset> {
    /** Amount of outer SDCA algorithm iterations. */
    private int amountOfIterations = 20;

    /** Amount of local SDCA algorithm iterations. */
    private int amountOfLocIterations = 50;

    /** Regularization parameter. */
    private double lambda = 0.2;

    /** This flag enables distributed mode for this algorithm. */
    private boolean isDistributed;

    /**
     * Returns model based on data
     *
     * @param data data to build model
     * @return model
     */
    @Override public SVMLinearBinaryClassificationModel train(LabeledDataset data) {
        isDistributed = data.isDistributed();

        final int weightVectorSizeWithIntercept = data.colSize() + 1;
        Vector weights = initializeWeightsWithZeros(weightVectorSizeWithIntercept);

        for (int i = 0; i < this.getAmountOfIterations(); i++) {
            Vector deltaWeights = calculateUpdates(data, weights);
            weights = weights.plus(deltaWeights); // creates new vector
        }

        return new SVMLinearBinaryClassificationModel(weights.viewPart(1, weights.size() - 1), weights.get(0));
    }

    /** */
    @NotNull private Vector initializeWeightsWithZeros(int vectorSize) {
        if (isDistributed)
            return new SparseDistributedVector(vectorSize);
        else
            return new DenseLocalOnHeapVector(vectorSize);
    }

    /** */
    private Vector calculateUpdates(LabeledDataset data, Vector weights) {
        Vector copiedWeights = weights.copy();
        Vector deltaWeights = initializeWeightsWithZeros(weights.size());

        final int amountOfObservation = data.rowSize();

        Vector tmpAlphas = initializeWeightsWithZeros(amountOfObservation);
        Vector deltaAlphas = initializeWeightsWithZeros(amountOfObservation);

        for (int i = 0; i < this.getAmountOfLocIterations(); i++) {
            int randomIdx = ThreadLocalRandom.current().nextInt(amountOfObservation);

            Deltas deltas = getDeltas(data, copiedWeights, amountOfObservation, tmpAlphas, randomIdx);

            copiedWeights = copiedWeights.plus(deltas.deltaWeights); // creates new vector
            deltaWeights = deltaWeights.plus(deltas.deltaWeights);  // creates new vector

            tmpAlphas.set(randomIdx, tmpAlphas.get(randomIdx) + deltas.deltaAlpha);
            deltaAlphas.set(randomIdx, deltaAlphas.get(randomIdx) + deltas.deltaAlpha);
        }
        return deltaWeights;
    }

    /** */
    private Deltas getDeltas(LabeledDataset data, Vector copiedWeights, int amountOfObservation, Vector tmpAlphas,
        int randomIdx) {
        LabeledVector row = (LabeledVector)data.getRow(randomIdx);
        Double lb = (Double)row.label();
        Vector v = makeVectorWithInterceptElement(row);

        double alpha = tmpAlphas.get(randomIdx);

        return maximize(lb, v, alpha, copiedWeights, amountOfObservation);
    }

    /** */
    private Vector makeVectorWithInterceptElement(LabeledVector row) {
        Vector vec = row.features().like(row.features().size() + 1);

        vec.set(0, 1); // set intercept element

        for (int j = 0; j < row.features().size(); j++)
            vec.set(j + 1, row.features().get(j));

        return vec;
    }

    /** */
    private Deltas maximize(double lb, Vector v, double alpha, Vector weights, int amountOfObservation) {
        double gradient = calcGradient(lb, v, weights, amountOfObservation);
        double prjGrad = calculateProjectionGradient(alpha, gradient);

        return calcDeltas(lb, v, alpha, prjGrad, weights.size(), amountOfObservation);
    }

    /** */
    private Deltas calcDeltas(double lb, Vector v, double alpha, double gradient, int vectorSize,
        int amountOfObservation) {
        if (gradient != 0.0) {

            double qii = v.dot(v);
            double newAlpha = calcNewAlpha(alpha, gradient, qii);

            Vector deltaWeights = v.times(lb * (newAlpha - alpha) / (this.lambda() * amountOfObservation));

            return new Deltas(newAlpha - alpha, deltaWeights);
        }
        else
            return new Deltas(0.0, initializeWeightsWithZeros(vectorSize));
    }

    /** */
    private double calcNewAlpha(double alpha, double gradient, double qii) {
        if (qii != 0.0)
            return Math.min(Math.max(alpha - (gradient / qii), 0.0), 1.0);
        else
            return 1.0;
    }

    /** */
    private double calcGradient(double lb, Vector v, Vector weights, int amountOfObservation) {
        double dotProduct = v.dot(weights);
        return (lb * dotProduct - 1.0) * (this.lambda() * amountOfObservation);
    }

    /** */
    private double calculateProjectionGradient(double alpha, double gradient) {
        if (alpha <= 0.0)
            return Math.min(gradient, 0.0);

        else if (alpha >= 1.0)
            return Math.max(gradient, 0.0);

        else
            return gradient;
    }

    /**
     * Set up the regularization parameter.
     * @param lambda The regularization parameter. Should be more than 0.0.
     * @return Trainer with new lambda parameter value.
     */
    public SVMLinearBinaryClassificationTrainer withLambda(double lambda) {
        assert lambda > 0.0;
        this.lambda = lambda;
        return this;
    }

    /**
     * Gets the regularization lambda.
     * @return The parameter value.
     */
    public double lambda() {
        return lambda;
    }

    /**
     * Gets the amount of outer iterations of SCDA algorithm.
     * @return The parameter value.
     */
    public int getAmountOfIterations() {
        return amountOfIterations;
    }

    /**
     * Set up the amount of outer iterations of SCDA algorithm.
     * @param amountOfIterations The parameter value.
     * @return Trainer with new amountOfIterations parameter value.
     */
    public SVMLinearBinaryClassificationTrainer withAmountOfIterations(int amountOfIterations) {
        this.amountOfIterations = amountOfIterations;
        return this;
    }

    /**
     * Gets the amount of local iterations of SCDA algorithm.
     * @return The parameter value.
     */
    public int getAmountOfLocIterations() {
        return amountOfLocIterations;
    }

    /**
     * Set up the amount of local iterations of SCDA algorithm.
     * @param amountOfLocIterations The parameter value.
     * @return Trainer with new amountOfLocIterations parameter value.
     */
    public SVMLinearBinaryClassificationTrainer withAmountOfLocIterations(int amountOfLocIterations) {
        this.amountOfLocIterations = amountOfLocIterations;
        return this;
    }
}

/** This is a helper class to handle pair results which are returned from the calculation method. */
class Deltas {
    /** */
    public double deltaAlpha;

    /** */
    public Vector deltaWeights;

    /** */
    public Deltas(double deltaAlpha, Vector deltaWeights) {
        this.deltaAlpha = deltaAlpha;
        this.deltaWeights = deltaWeights;
    }
}


