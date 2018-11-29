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
import java.nio.file.Path;
import java.util.Scanner;
import org.apache.ignite.ml.inference.InfModel;
import org.apache.ignite.ml.inference.builder.SingleInfModelBuilder;
import org.apache.ignite.ml.inference.builder.SyncInfModelBuilder;
import org.apache.ignite.ml.inference.reader.FileSystemInfModelReader;
import org.apache.ignite.ml.inference.reader.InfModelReader;
import org.apache.ignite.ml.xgboost.MapBasedXGObject;
import org.apache.ignite.ml.xgboost.XGModel;
import org.apache.ignite.ml.xgboost.XGObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link XGBoostModelParser}.
 */
public class XGBoostModelParserTest {
    /** Test model resource name. */
    private static final String TEST_MODEL_RESOURCE = "agaricus-model.txt";

    /** Parser. */
    private final XGModelParser parser = new XGModelParser();

    /** Model builder. */
    private final SyncInfModelBuilder mdlBuilder = new SingleInfModelBuilder();

    /** End-to-end test for {@code parse()} and {@code predict()} methods. */
    @Test
    public void testParseAndPredict() {
        URL url = XGBoostModelParserTest.class.getClassLoader().getResource(TEST_MODEL_RESOURCE);
        if (url == null)
            throw new IllegalStateException("File not found [resource_name=" + TEST_MODEL_RESOURCE + "]");

        InfModelReader reader = new FileSystemInfModelReader(url.getPath());

        try (InfModel<XGObject, Double> mdl = mdlBuilder.build(reader, parser);
             Scanner testDataScanner = new Scanner(XGBoostModelParserTest.class.getClassLoader()
                 .getResourceAsStream("agaricus-test-data.txt"));
             Scanner testExpResultsScanner = new Scanner(XGBoostModelParserTest.class.getClassLoader()
                 .getResourceAsStream("agaricus-test-expected-results.txt"))) {

            while (testDataScanner.hasNextLine()) {
                assertTrue(testExpResultsScanner.hasNextLine());

                String testDataStr = testDataScanner.nextLine();
                String testExpResultsStr = testExpResultsScanner.nextLine();

                MapBasedXGObject testObj = new MapBasedXGObject();

                for (String keyValueString : testDataStr.split(" ")) {
                    String[] keyVal = keyValueString.split(":");

                    if (keyVal.length == 2)
                        testObj.put("f" + keyVal[0], Double.parseDouble(keyVal[1]));
                }

                double prediction = mdl.predict(testObj);

                double expPrediction = Double.parseDouble(testExpResultsStr);

                assertEquals(expPrediction, prediction, 1e-6);
            }
        }
    }
}