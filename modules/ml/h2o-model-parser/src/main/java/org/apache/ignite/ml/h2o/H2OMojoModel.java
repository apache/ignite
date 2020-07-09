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

package org.apache.ignite.ml.h2o;

import hex.genmodel.easy.EasyPredictModelWrapper;
import hex.genmodel.easy.RowData;
import hex.genmodel.easy.exception.PredictException;
import hex.genmodel.easy.prediction.AbstractPrediction;
import hex.genmodel.easy.prediction.BinomialModelPrediction;
import hex.genmodel.easy.prediction.ClusteringModelPrediction;
import hex.genmodel.easy.prediction.MultinomialModelPrediction;
import hex.genmodel.easy.prediction.OrdinalModelPrediction;
import hex.genmodel.easy.prediction.RegressionModelPrediction;
import org.apache.ignite.ml.inference.Model;
import org.apache.ignite.ml.math.primitives.vector.NamedVector;

/**
 * H2O MOJO Model imported and wrapped to be compatible with Apache Ignite infrastructure.
 */
public class H2OMojoModel implements Model<NamedVector, Double> {
    /** H2O MOJO model (accessible using in EasyPredict API). */
    private final EasyPredictModelWrapper easyPredict;

    /**
     * Constructs a new instance of H2O MOJO model wrapper.
     *
     * @param easyPredict H2O MOJO Model
     */
    public H2OMojoModel(EasyPredictModelWrapper easyPredict) {
        this.easyPredict = easyPredict;
    }

    /** {@inheritDoc} */
    @Override public Double predict(NamedVector input) {
        RowData rowData = toRowData(input);
        try {
            AbstractPrediction prediction = easyPredict.predict(rowData);
            return extractRawValue(prediction);
        } catch (PredictException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Extracts the raw value.
     * @param prediction Prediction.
     */
    private static double extractRawValue(AbstractPrediction prediction) {
        if (prediction instanceof BinomialModelPrediction)
            return ((BinomialModelPrediction) prediction).labelIndex;
        else if (prediction instanceof MultinomialModelPrediction)
            return ((MultinomialModelPrediction) prediction).labelIndex;
        else if (prediction instanceof RegressionModelPrediction)
            return ((RegressionModelPrediction) prediction).value;
        else if (prediction instanceof OrdinalModelPrediction)
            return ((OrdinalModelPrediction) prediction).labelIndex;
        else if (prediction instanceof ClusteringModelPrediction)
            return ((ClusteringModelPrediction) prediction).cluster;
        else
            throw new UnsupportedOperationException("Prediction " + prediction + " cannot be converted to a raw value.");
    }

    /**
     * Converts the named vector to row data.
     * @param input Input.
     */
    private static RowData toRowData(NamedVector input) {
        RowData row = new RowData();
        for (String key : input.getKeys())
            row.put(key, input.get(key));
        return row;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // nothing to do
    }
}
