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

package org.apache.ignite.examples.ml.xgboost;

import java.net.URL;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.ml.inference.InfModel;
import org.apache.ignite.ml.inference.builder.AsyncInfModelBuilder;
import org.apache.ignite.ml.inference.builder.IgniteDistributedInfModelBuilder;
import org.apache.ignite.ml.inference.reader.FileSystemInfModelReader;
import org.apache.ignite.ml.inference.reader.InfModelReader;
import org.apache.ignite.ml.xgboost.MapBasedXGObject;
import org.apache.ignite.ml.xgboost.XGObject;
import org.apache.ignite.ml.xgboost.parser.XGModelParser;

/**
 * This example demonstrates how to import XGBoost model and use imported model for distributed inference in Apache
 * Ignite.
 */
public class XGBoostModelParserExample {
    /** Test model resource name. */
    private static final String TEST_MODEL_RESOURCE = "models/xgboost/agaricus-model.txt";

    /** Parser. */
    private static final XGModelParser parser = new XGModelParser();

    /** Run example. */
    public static void main(String... args) throws ExecutionException, InterruptedException {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            URL url = XGBoostModelParserExample.class.getClassLoader().getResource(TEST_MODEL_RESOURCE);
            if (url == null)
                throw new IllegalStateException("File not found [resource_name=" + TEST_MODEL_RESOURCE + "]");

            InfModelReader reader = new FileSystemInfModelReader(url.getPath());

            AsyncInfModelBuilder mdlBuilder =  new IgniteDistributedInfModelBuilder(ignite, 4, 4);

            try (InfModel<XGObject, Future<Double>> mdl = mdlBuilder.build(reader, parser);
                 Scanner testDataScanner = new Scanner(XGBoostModelParserExample.class.getClassLoader()
                     .getResourceAsStream("datasets/agaricus-test-data.txt"));
                 Scanner testExpResultsScanner = new Scanner(XGBoostModelParserExample.class.getClassLoader()
                     .getResourceAsStream("datasets/agaricus-test-expected-results.txt"))) {

                while (testDataScanner.hasNextLine()) {
                    String testDataStr = testDataScanner.nextLine();
                    String testExpResultsStr = testExpResultsScanner.nextLine();

                    MapBasedXGObject testObj = new MapBasedXGObject();

                    for (String keyValueString : testDataStr.split(" ")) {
                        String[] keyVal = keyValueString.split(":");

                        if (keyVal.length == 2)
                            testObj.put("f" + keyVal[0], Double.parseDouble(keyVal[1]));
                    }

                    double prediction = mdl.predict(testObj).get();

                    double expPrediction = Double.parseDouble(testExpResultsStr);

                    System.out.println("Expected: " + expPrediction + ", prediction: " + prediction);
                }
            }
        }
    }
}
