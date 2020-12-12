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

package org.apache.ignite.examples.ml.inference.catboost;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.catboost.CatboostClassificationModelParser;
import org.apache.ignite.ml.inference.Model;
import org.apache.ignite.ml.inference.builder.AsyncModelBuilder;
import org.apache.ignite.ml.inference.builder.IgniteDistributedModelBuilder;
import org.apache.ignite.ml.inference.reader.FileSystemModelReader;
import org.apache.ignite.ml.inference.reader.ModelReader;
import org.apache.ignite.ml.math.primitives.vector.NamedVector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;

/**
 * This example demonstrates how to import Catboost model and use imported model for distributed inference in Apache
 * Ignite.
 */
public class CatboostClassificationModelParserExample {
    /**
     * Test model resource name.
     */
    private static final String TEST_MODEL_RES = "examples/src/main/resources/models/catboost/model_clf.cbm";

    /**
     * Test data.
     */
    private static final String TEST_DATA_RES = "examples/src/main/resources/datasets/amazon-employee-access-challenge-sample.csv";

    /**
     * Test expected results.
     */
    private static final String TEST_ER_RES = "examples/src/main/resources/datasets/amazon-employee-access-challenge-sample-catboost-expected-results.csv";

    /**
     * Parser.
     */
    private static final CatboostClassificationModelParser parser = new CatboostClassificationModelParser();

    /**
     * Run example.
     */
    public static void main(String... args) throws ExecutionException, InterruptedException,
        FileNotFoundException {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            File mdlRsrc = IgniteUtils.resolveIgnitePath(TEST_MODEL_RES);
            if (mdlRsrc == null)
                throw new IllegalArgumentException("File not found [resource_path=" + TEST_MODEL_RES + "]");

            ModelReader reader = new FileSystemModelReader(mdlRsrc.getPath());

            AsyncModelBuilder mdlBuilder = new IgniteDistributedModelBuilder(ignite, 4, 4);

            File testData = IgniteUtils.resolveIgnitePath(TEST_DATA_RES);
            if (testData == null)
                throw new IllegalArgumentException("File not found [resource_path=" + TEST_DATA_RES + "]");

            File testExpRes = IgniteUtils.resolveIgnitePath(TEST_ER_RES);
            if (testExpRes == null)
                throw new IllegalArgumentException("File not found [resource_path=" + TEST_ER_RES + "]");

            try (Model<NamedVector, Future<Double>> mdl = mdlBuilder.build(reader, parser);
                 Scanner testDataScanner = new Scanner(testData);
                 Scanner testExpResultsScanner = new Scanner(testExpRes)) {
                String header = testDataScanner.nextLine();
                String[] columns = header.split(",");

                while (testDataScanner.hasNextLine()) {
                    String testDataStr = testDataScanner.nextLine();
                    String testExpResultsStr = testExpResultsScanner.nextLine();

                    HashMap<String, Double> testObj = new HashMap<>();
                    String[] values = testDataStr.split(",");

                    for (int i = 0; i < columns.length; i++) {
                      testObj.put(columns[i], Double.valueOf(values[i]));
                    }

                    double prediction = mdl.predict(VectorUtils.of(testObj)).get();
                    double expPrediction = Double.parseDouble(testExpResultsStr);

                    System.out.println("Expected: " + expPrediction + ", prediction: " + prediction);
                }
            }
        }
        finally {
            System.out.flush();
        }
  }
}
