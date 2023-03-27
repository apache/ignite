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

package org.apache.ignite.ml.xgboost.parser;

import java.net.URL;
import java.util.HashMap;
import java.util.Scanner;
import org.apache.ignite.ml.inference.builder.SingleModelBuilder;
import org.apache.ignite.ml.inference.builder.SyncModelBuilder;
import org.apache.ignite.ml.inference.reader.FileSystemModelReader;
import org.apache.ignite.ml.inference.reader.ModelReader;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.xgboost.XGModelComposition;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link XGBoostModelParser}.
 */
public class XGBoostModelParserTest {
    /** Test model resource name. */
    private static final String TEST_MODEL_RESOURCE = "datasets/agaricus-model.txt";

    /** Parser. */
    private final XGModelParser parser = new XGModelParser();

    /** Model builder. */
    private final SyncModelBuilder mdlBuilder = new SingleModelBuilder();

    /** End-to-end test for {@code parse()} and {@code predict()} methods. */
    @Test
    public void testParseAndPredict() {
        URL url = XGBoostModelParserTest.class.getClassLoader().getResource(TEST_MODEL_RESOURCE);
        if (url == null)
            throw new IllegalStateException("File not found [resource_name=" + TEST_MODEL_RESOURCE + "]");

        ModelReader reader = new FileSystemModelReader(url.getPath());

        try (XGModelComposition mdl = mdlBuilder.build(reader, parser);
             Scanner testDataScanner = new Scanner(XGBoostModelParserTest.class.getClassLoader()
                 .getResourceAsStream("datasets/agaricus-test-data.txt"));
             Scanner testExpResultsScanner = new Scanner(XGBoostModelParserTest.class.getClassLoader()
                 .getResourceAsStream("datasets/agaricus-test-expected-results.txt"))) {

            while (testDataScanner.hasNextLine()) {
                assertTrue(testExpResultsScanner.hasNextLine());

                String testDataStr = testDataScanner.nextLine();
                String testExpResultsStr = testExpResultsScanner.nextLine();

                HashMap<String, Double> testObj = new HashMap<>();

                for (String keyValueString : testDataStr.split(" ")) {
                    String[] keyVal = keyValueString.split(":");

                    if (keyVal.length == 2)
                        testObj.put("f" + keyVal[0], Double.parseDouble(keyVal[1]));
                }

                double prediction = mdl.predict(VectorUtils.of(testObj));

                double expPrediction = Double.parseDouble(testExpResultsStr);

                assertEquals(expPrediction, prediction, 1e-6);
            }
        }
    }
}
