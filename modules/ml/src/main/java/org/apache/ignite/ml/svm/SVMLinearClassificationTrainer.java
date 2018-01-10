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

import com.sun.tools.doclint.Entity;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.ml.Trainer;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.functions.IgniteDoubleFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.structures.DatasetRow;
import org.apache.ignite.ml.structures.LabeledDataset;
import org.apache.ignite.ml.structures.LabeledVector;
import org.jetbrains.annotations.NotNull;


/**
 * Base class for linear classification models.
 */
public class SVMLinearClassificationTrainer implements Trainer<SVMLinearClassificationModel, LabeledDataset> {

    private Vector weights;
    private List<Vector> alphas;
    private double intercept = 0.0;

    /**
     * Returns model based on data
     *
     * @param data data to build model
     * @return model
     */
    @Override public SVMLinearClassificationModel train(LabeledDataset data) {

        weights = initializeWeightsWithZeros(data.colSize());
        alphas  = initializeAlphasWithZeros(data.colSize());

        SVMLinearClassificationModel mdl = new SVMLinearClassificationModel(weights, intercept);

        for (int i = 0; i < mdl.getAmountOfIterations(); i++) {
            Vector deltaWeights = calculateUpdates(data, mdl, data.colSize()); //DEBUG: maybe should be scaled or normalized
            weights.plus(deltaWeights);
        }

        return mdl;
    }

    // DEBUG: rename or change signature
    @NotNull private Vector initializeWeightsWithZeros(int vectorSize) {
        return new DenseLocalOnHeapVector(vectorSize);
    }

    // DEBUG: rename or change signature
    @NotNull private List<Vector> initializeAlphasWithZeros (int vectorSize) {
        List<Vector> localAlphas = new ArrayList<>();
        localAlphas.add(new DenseLocalOnHeapVector(vectorSize));
        return localAlphas;
    }


    private Vector calculateUpdates(LabeledDataset data, SVMLinearClassificationModel mdl, int vectorSize) {
        Vector copiedWeights = weights.copy();
        Vector deltaWeights = initializeWeightsWithZeros(weights.size()); // initialize with zeros

        Vector tmpAlphas = alphas.get(alphas.size()-1).copy();
        Vector deltaAlphas = new DenseLocalOnHeapVector(vectorSize); // initialize with zeros

        int amountOfLocIterations = 10; // should be part of the model
        for (int i = 0; i < amountOfLocIterations; i++) {
            int randomIdx = ThreadLocalRandom.current().nextInt(data.rowSize());
            LabeledVector row = (LabeledVector)data.getRow(randomIdx);
            double alpha = tmpAlphas.get(randomIdx);

            Deltas deltas = maximize(row, alpha, copiedWeights, mdl, data.rowSize());

            copiedWeights.plus(deltas.deltaWeights);
            deltaWeights.plus(deltas.deltaWeights);

            tmpAlphas.set(randomIdx, tmpAlphas.get(randomIdx) + deltas.deltaAlpha);
            deltaAlphas.set(randomIdx, deltaAlphas.get(randomIdx) + deltas.deltaAlpha);
        }

        return deltaWeights;
    }

    private Deltas maximize(LabeledVector row, double alpha, Vector weights,
        SVMLinearClassificationModel mdl, int amountOfObservation) {

        double gradient = caclGradient(row, weights, mdl, amountOfObservation);
        double projectionGrad = calculateProjectionGradient(alpha, gradient);

        Deltas deltas = calcDeltas(row, alpha, gradient, weights.size(), mdl, amountOfObservation);

        return deltas;

    }

    private Deltas calcDeltas(LabeledVector row, double alpha, double gradient, int vectorSize,
        SVMLinearClassificationModel mdl,
        int amountOfObservation) {
        if(gradient != 0.0) {

            double qii = row.features().dot(row.features());
            double newAlpha = caclNewAlpha(alpha, gradient, qii);

            Vector deltaWeights = row.features().times((double)row.label()).times(newAlpha - alpha)
                .divide(mdl.getRegularization() * amountOfObservation);
            return new Deltas (newAlpha - alpha, deltaWeights);
        } else
            return new Deltas(0.0, initializeWeightsWithZeros(vectorSize));

    }

    private double caclNewAlpha(double alpha, double gradient, double qii) {
        if(qii != 0.0)
            return Math.min(Math.max(alpha - (gradient/qii), 0.0), 1.0);
        return 1.0;
    }

    private double caclGradient(LabeledVector row, Vector weights, SVMLinearClassificationModel mdl,
        int amountOfObservation) {
        double dotProduct = row.features().dot(weights);
        return ((double)row.label() * dotProduct - 1.0) * (mdl.getRegularization() * amountOfObservation);
    }

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


