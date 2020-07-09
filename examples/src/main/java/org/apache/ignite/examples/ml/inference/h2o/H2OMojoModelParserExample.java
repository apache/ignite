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

package org.apache.ignite.examples.ml.inference.h2o;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.h2o.H2OMojoModelParser;
import org.apache.ignite.ml.inference.Model;
import org.apache.ignite.ml.inference.builder.AsyncModelBuilder;
import org.apache.ignite.ml.inference.builder.IgniteDistributedModelBuilder;
import org.apache.ignite.ml.inference.reader.FileSystemModelReader;
import org.apache.ignite.ml.inference.reader.ModelReader;
import org.apache.ignite.ml.math.primitives.vector.NamedVector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;

/**
 * This example demonstrates how to import H2O MOJO model and use imported model for distributed inference in Apache
 * Ignite.
 */
public class H2OMojoModelParserExample {
    /**
     * Test model resource name.
     */
    private static final String MODEL_RES = "examples/src/main/resources/models/h2o/agaricus-gbm-mojo.zip";

    /**
     * Test data.
     */
    private static final String DATA_RES = "examples/src/main/resources/datasets/agaricus-test-data.txt";

    /**
     * Parser.
     */
    private static final H2OMojoModelParser parser = new H2OMojoModelParser();

    /**
     * Run example.
     */
    public static void main(String... args) throws ExecutionException, InterruptedException, FileNotFoundException {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            File mdlRsrc = IgniteUtils.resolveIgnitePath(MODEL_RES);
            if (mdlRsrc == null)
                throw new IllegalArgumentException("File not found [resource_path=" + MODEL_RES + "]");

            ModelReader reader = new FileSystemModelReader(mdlRsrc.getPath());

            AsyncModelBuilder mdlBuilder = new IgniteDistributedModelBuilder(ignite, 4, 4);

            File testData = IgniteUtils.resolveIgnitePath(DATA_RES);
            if (testData == null)
                throw new IllegalArgumentException("File not found [resource_path=" + DATA_RES + "]");

            try (Model<NamedVector, Future<Double>> mdl = mdlBuilder.build(reader, parser);
                 Scanner testDataScanner = new Scanner(testData)) {

                while (testDataScanner.hasNextLine()) {
                    String testDataStr = testDataScanner.nextLine();
                    String actual = null;

                    HashMap<String, Double> testObj = new HashMap<>();

                    for (String keyValueString : testDataStr.split(" ")) {
                        String[] keyVal = keyValueString.split(":");

                        if (keyVal.length == 2)
                            testObj.put("C" + (1 + Integer.parseInt(keyVal[0])), Double.parseDouble(keyVal[1]));
                        else
                            actual = keyValueString;
                    }

                    double prediction = mdl.predict(VectorUtils.of(testObj)).get();

                    System.out.println("Actual: " + actual + ", prediction: " + prediction);
                }
            }
        }
        finally {
            System.out.flush();
        }
    }
}
