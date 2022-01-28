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

package org.apache.ignite.ml.catboost.parser;

import java.net.URL;
import java.util.HashMap;

import org.apache.ignite.ml.catboost.CatboostRegressionModel;
import org.apache.ignite.ml.catboost.CatboostRegressionModelParser;
import org.apache.ignite.ml.inference.builder.SingleModelBuilder;
import org.apache.ignite.ml.inference.builder.SyncModelBuilder;
import org.apache.ignite.ml.inference.reader.FileSystemModelReader;
import org.apache.ignite.ml.inference.reader.ModelReader;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link CatboostRegressionModelParser}.
 */
public class CatboostRegressionModelParserTest {
    /** Test model resource name. */
    private static final String TEST_MODEL_RESOURCE = "models/model_reg.cbm";

    /** Parser. */
    private final CatboostRegressionModelParser parser = new CatboostRegressionModelParser();

    /** Model builder. */
    private final SyncModelBuilder mdlBuilder = new SingleModelBuilder();

    /** End-to-end test for {@code parse()} and {@code predict()} methods. */
    @Test
    public void testParseAndPredict() {
        URL url = CatboostRegressionModelParserTest.class.getClassLoader().getResource(TEST_MODEL_RESOURCE);
        if (url == null)
            throw new IllegalStateException("File not found [resource_name=" + TEST_MODEL_RESOURCE + "]");

        ModelReader reader = new FileSystemModelReader(url.getPath());

        try (CatboostRegressionModel mdl = mdlBuilder.build(reader, parser)) {
            HashMap<String, Double> input = new HashMap<>();
            input.put("f_0", 0.02731d);
            input.put("f_1", 0.0d);
            input.put("f_2", 7.07d);
            input.put("f_3", 0d);
            input.put("f_4", 0.469d);
            input.put("f_5", 6.421d);
            input.put("f_6", 78.9d);
            input.put("f_7", 4.9671d);
            input.put("f_8", 2d);
            input.put("f_9", 242.0d);
            input.put("f_10", 17.8d);
            input.put("f_11", 396.9d);
            input.put("f_12", 9.14d);
            double prediction = mdl.predict(VectorUtils.of(input));

            assertEquals(21.164552741740483, prediction, 1e-5);
        }
    }
}
