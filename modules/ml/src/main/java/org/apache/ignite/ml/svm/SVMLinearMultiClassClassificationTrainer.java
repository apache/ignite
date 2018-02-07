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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.ml.Trainer;
import org.apache.ignite.ml.structures.LabeledDataset;

/**
 * Base class for a soft-margin SVM linear multiclass-classification trainer based on the communication-efficient
 * distributed dual coordinate ascent algorithm (CoCoA) with hinge-loss function.
 *
 * All common parameters are shared with bunch of binary classification trainers.
 */
public class SVMLinearMultiClassClassificationTrainer implements Trainer<SVMLinearMultiClassClassificationModel, LabeledDataset> {
    /** Amount of outer SDCA algorithm iterations. */
    private int amountOfIterations = 20;

    /** Amount of local SDCA algorithm iterations. */
    private int amountOfLocIterations = 50;

    /** Regularization parameter. */
    private double lambda = 0.2;

    /**
     * Returns model based on data.
     *
     * @param data data to build model.
     * @return model.
     */
    @Override public SVMLinearMultiClassClassificationModel train(LabeledDataset data) {
        List<Double> classes = getClassLabels(data);

        SVMLinearMultiClassClassificationModel multiClsMdl = new SVMLinearMultiClassClassificationModel();

        classes.forEach(clsLb -> {
            LabeledDataset binarizedDataset = binarizeLabels(data, clsLb);

            SVMLinearBinaryClassificationTrainer trainer = new SVMLinearBinaryClassificationTrainer()
                .withAmountOfIterations(this.amountOfIterations())
                .withAmountOfLocIterations(this.amountOfLocIterations())
                .withLambda(this.lambda());

            multiClsMdl.add(clsLb, trainer.train(binarizedDataset));
        });

        return multiClsMdl;
    }

    /**
     * Copies the given data and changes class labels in +1 for chosen class and in -1 for the rest classes.
     *
     * @param data Data to transform.
     * @param clsLb Chosen class in schema One-vs-Rest.
     * @return Copy of dataset with new labels.
     */
    private LabeledDataset binarizeLabels(LabeledDataset data, double clsLb) {
        final LabeledDataset ds = data.copy();

        for (int i = 0; i < ds.rowSize(); i++)
            ds.setLabel(i, ds.label(i) == clsLb ? 1.0 : -1.0);

        return ds;
    }

    /** Iterates among dataset and collects class labels. */
    private List<Double> getClassLabels(LabeledDataset data) {
        final Set<Double> clsLabels = new HashSet<>();

        for (int i = 0; i < data.rowSize(); i++)
            clsLabels.add(data.label(i));

        List<Double> res = new ArrayList<>();
        res.addAll(clsLabels);

        return res;
    }

    /**
     * Set up the regularization parameter.
     *
     * @param lambda The regularization parameter. Should be more than 0.0.
     * @return Trainer with new lambda parameter value.
     */
    public SVMLinearMultiClassClassificationTrainer withLambda(double lambda) {
        assert lambda > 0.0;
        this.lambda = lambda;
        return this;
    }

    /**
     * Gets the regularization lambda.
     *
     * @return The parameter value.
     */
    public double lambda() {
        return lambda;
    }

    /**
     * Gets the amount of outer iterations of SCDA algorithm.
     *
     * @return The parameter value.
     */
    public int amountOfIterations() {
        return amountOfIterations;
    }

    /**
     * Set up the amount of outer iterations of SCDA algorithm.
     *
     * @param amountOfIterations The parameter value.
     * @return Trainer with new amountOfIterations parameter value.
     */
    public SVMLinearMultiClassClassificationTrainer withAmountOfIterations(int amountOfIterations) {
        this.amountOfIterations = amountOfIterations;
        return this;
    }

    /**
     * Gets the amount of local iterations of SCDA algorithm.
     *
     * @return The parameter value.
     */
    public int amountOfLocIterations() {
        return amountOfLocIterations;
    }

    /**
     * Set up the amount of local iterations of SCDA algorithm.
     *
     * @param amountOfLocIterations The parameter value.
     * @return Trainer with new amountOfLocIterations parameter value.
     */
    public SVMLinearMultiClassClassificationTrainer withAmountOfLocIterations(int amountOfLocIterations) {
        this.amountOfLocIterations = amountOfLocIterations;
        return this;
    }
}



