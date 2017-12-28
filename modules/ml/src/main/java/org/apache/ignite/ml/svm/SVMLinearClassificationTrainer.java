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

import java.io.Serializable;
import org.apache.ignite.ml.Exportable;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.Trainer;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.structures.LabeledDataset;

/**
 * Base class for linear classification models.
 */
public class SVMLinearClassificationTrainer implements Trainer<SVMLinearClassificationModel, LabeledDataset> {
    /**
     * Returns model based on data
     *
     * @param data data to build model
     * @return model
     */
    @Override public SVMLinearClassificationModel train(LabeledDataset data) {

        Vector initialWeights = new DenseLocalOnHeapVector(data.colSize());
        double intercept = 0.0;
        SVMLinearClassificationModel mdl = new SVMLinearClassificationModel(initialWeights, intercept);

        Vector deltaWeights;
        for (int i = 0; i < mdl.getAmountOfIterations(); i++) {
            deltaWeights = calculateUpdates(mdl, data.colSize()); // maybe should be scaled or normalized
            initialWeights.plus(deltaWeights);
        }




        return mdl;
    }

    private Vector calculateUpdates(SVMLinearClassificationModel mdl, int vectorSize) {
    }
}
