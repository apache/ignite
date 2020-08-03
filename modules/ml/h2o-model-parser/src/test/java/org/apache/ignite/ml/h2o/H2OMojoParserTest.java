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

import java.net.URL;
import java.util.HashMap;

import org.apache.ignite.ml.inference.builder.SingleModelBuilder;
import org.apache.ignite.ml.inference.builder.SyncModelBuilder;
import org.apache.ignite.ml.inference.reader.FileSystemModelReader;
import org.apache.ignite.ml.inference.reader.ModelReader;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link H2OMojoModelParser}.
 */
public class H2OMojoParserTest {
    /** Test model resource name. */
    private static final String TEST_MODEL_RESOURCE = "mojos/gbm_prostate.zip";

    /** Parser. */
    private final H2OMojoModelParser parser = new H2OMojoModelParser();

    /** Model builder. */
    private final SyncModelBuilder mdlBuilder = new SingleModelBuilder();

    /** */
    @Test
    public void testParseAndPredict() {
        URL url = H2OMojoParserTest.class.getClassLoader().getResource(TEST_MODEL_RESOURCE);
        if (url == null)
            throw new IllegalStateException("File not found [resource_name=" + TEST_MODEL_RESOURCE + "]");

        ModelReader reader = new FileSystemModelReader(url.getPath());

        try (H2OMojoModel mdl = mdlBuilder.build(reader, parser)) {
            HashMap<String, Double> input = new HashMap<>();
            input.put("RACE", 1.0);
            input.put("DPROS", 2.0);
            input.put("DCAPS", 1.0);
            input.put("PSA", 1.4);
            input.put("VOL", 0.0);
            input.put("GLEASON", 6.0);

            double prediction = mdl.predict(VectorUtils.of(input));

            assertEquals(64.50328, prediction, 1e-5);
        }
    }
}
