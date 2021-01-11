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

package org.apache.ignite.ml.catboost;

import ai.catboost.CatBoostError;
import ai.catboost.CatBoostModel;
import org.apache.ignite.ml.inference.Model;
import org.apache.ignite.ml.math.primitives.vector.NamedVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CatboostClassificationModel imported and wrapped to be compatible with Apache Ignite infrastructure.
 */
public class CatboostClassificationModel implements Model<NamedVector, Double> {
    /** Logger. */
    private static final Logger logger = LoggerFactory.getLogger(CatboostClassificationModel.class);

    /** Catboost model. */
    private final CatBoostModel model;

    /**
     * Constructs a new instance of Catboost model wrapper.
     *
     * @param model Catboost Model
     */
    public CatboostClassificationModel(CatBoostModel model) {
        this.model = model;
    }

    /** {@inheritDoc} */
    @Override public Double predict(NamedVector input) {
        float[] floatInput = new float[input.size()];
        int index = 0;
        for (String key: model.getFeatureNames()) {
            floatInput[index] = (float) input.get(key);
            index++;
        }

        try {
            double predict = model.predict(floatInput, model.getFeatureNames())
                .get(0, 0);
            // use formula based on https://github.com/catboost/benchmarks/blob/61d62512f751325a14dd885bb71f8c2dabf7e24b/quality_benchmarks/catboost_experiment.py#L77
            return Math.pow(1 + Math.exp(-predict), -1);
        } catch (CatBoostError e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    /** {@inheritDoc} */
    @Override public void close() {
        try {
          model.close();
        } catch (CatBoostError e) {
          logger.error(e.getMessage());
        }
    }
}
