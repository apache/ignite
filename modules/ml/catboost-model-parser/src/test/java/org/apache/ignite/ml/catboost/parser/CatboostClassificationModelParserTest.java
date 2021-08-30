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

import org.apache.ignite.ml.catboost.CatboostClassificationModel;
import org.apache.ignite.ml.catboost.CatboostClassificationModelParser;
import org.apache.ignite.ml.inference.builder.SingleModelBuilder;
import org.apache.ignite.ml.inference.builder.SyncModelBuilder;
import org.apache.ignite.ml.inference.reader.FileSystemModelReader;
import org.apache.ignite.ml.inference.reader.ModelReader;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link CatboostClassificationModelParser}.
 */
public class CatboostClassificationModelParserTest {
    /** Test model resource name. */
    private static final String TEST_MODEL_RESOURCE = "models/model_clf.cbm";

    /** Parser. */
    private final CatboostClassificationModelParser parser = new CatboostClassificationModelParser();

    /** Model builder. */
    private final SyncModelBuilder mdlBuilder = new SingleModelBuilder();

    /** End-to-end test for {@code parse()} and {@code predict()} methods. */
    @Test
    public void testParseAndPredict() {
        URL url = CatboostClassificationModelParserTest.class.getClassLoader().getResource(TEST_MODEL_RESOURCE);
        if (url == null)
            throw new IllegalStateException("File not found [resource_name=" + TEST_MODEL_RESOURCE + "]");

        ModelReader reader = new FileSystemModelReader(url.getPath());

        try (
            CatboostClassificationModel mdl = mdlBuilder.build(reader, parser)) {
            HashMap<String, Double> input = new HashMap<>();
            input.put("ACTION", 1.0);
            input.put("RESOURCE", 39353.0);
            input.put("MGR_ID", 85475.0);
            input.put("ROLE_ROLLUP_1", 117961.0);
            input.put("ROLE_ROLLUP_2", 118300.0);
            input.put("ROLE_DEPTNAME", 123472.0);
            input.put("ROLE_TITLE", 117905.0);
            input.put("ROLE_FAMILY_DESC", 117906.0);
            input.put("ROLE_FAMILY", 290919.0);
            input.put("ROLE_CODE", 117908.0);
            double prediction = mdl.predict(VectorUtils.of(input));

            assertEquals(0.9928904609329371, prediction, 1e-5);
        }
    }
}
