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

package org.apache.ignite.ml.sql;

import java.util.Arrays;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.ml.inference.IgniteModelStorageUtil;
import org.apache.ignite.ml.inference.Model;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;

/**
 * SQL functions that should be defined and passed into cache configuration to extend list of functions available
 * in SQL interface.
 */
public class SQLFunctions {
    /**
     * Makes prediction using specified model name to extract model from model storage and specified input values
     * as input object for prediction.
     *
     * @param mdl Pretrained model.
     * @param x Input values.
     * @return Prediction.
     */
    @QuerySqlFunction
    public static double predict(String mdl, Double... x) {
        System.out.println("Prediction for " + Arrays.toString(x));

        try (Model<Vector, Double> infMdl = IgniteModelStorageUtil.getModel(mdl)) {
            return infMdl.predict(VectorUtils.of(x));
        }
    }
}
