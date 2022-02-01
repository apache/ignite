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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import ai.catboost.CatBoostError;
import ai.catboost.CatBoostModel;
import org.apache.ignite.ml.inference.parser.ModelParser;
import org.apache.ignite.ml.math.primitives.vector.NamedVector;

/**
 * Catboost Classification model parser.
 */
public class CatboostClassificationModelParser implements
    ModelParser<NamedVector, Double, CatboostClassificationModel> {
    /** */
    private static final long serialVersionUID = -8425510352746936163L;

    /** {@inheritDoc} */
    @Override public CatboostClassificationModel parse(byte[] mdl) {
        try (InputStream inputStream = new ByteArrayInputStream(mdl)) {
            return new CatboostClassificationModel(CatBoostModel.loadModel(inputStream));
        } catch (IOException | CatBoostError e) {
            throw new RuntimeException("Failed to parse model " + e.getMessage(), e);
        }
    }
}
