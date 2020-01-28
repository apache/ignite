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

package org.apache.ignite.examples.ml.inference;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.examples.ml.regression.linear.LinearRegressionLSQRTrainerExample;
import org.apache.ignite.examples.ml.util.MLSandboxDatasets;
import org.apache.ignite.examples.ml.util.SandboxMLCache;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.inference.Model;
import org.apache.ignite.ml.inference.builder.IgniteDistributedModelBuilder;
import org.apache.ignite.ml.inference.parser.IgniteModelParser;
import org.apache.ignite.ml.inference.parser.ModelParser;
import org.apache.ignite.ml.inference.reader.InMemoryModelReader;
import org.apache.ignite.ml.inference.reader.ModelReader;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.regressions.linear.LinearRegressionLSQRTrainer;
import org.apache.ignite.ml.regressions.linear.LinearRegressionModel;

/**
 * This example is based on {@link LinearRegressionLSQRTrainerExample}, but to perform inference it uses an approach
 * implemented in {@link org.apache.ignite.ml.inference} package.
 */
public class IgniteModelDistributedInferenceExample {
    /**
     * Run example.
     */
    public static void main(String... args) throws IOException, ExecutionException, InterruptedException {
        System.out.println();
        System.out.println(">>> Linear regression model over cache based dataset usage example started.");
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteCache<Integer, Vector> dataCache = null;
            try {
                dataCache = new SandboxMLCache(ignite).fillCacheWith(MLSandboxDatasets.MORTALITY_DATA);

                System.out.println(">>> Create new linear regression trainer object.");
                LinearRegressionLSQRTrainer trainer = new LinearRegressionLSQRTrainer();

                System.out.println(">>> Perform the training to get the model.");
                LinearRegressionModel mdl = trainer.fit(
                    ignite,
                    dataCache,
                    new DummyVectorizer<Integer>().labeled(Vectorizer.LabelCoordinate.FIRST)
                );

                System.out.println(">>> Linear regression model: " + mdl);

                System.out.println(">>> Preparing model reader and model parser.");
                ModelReader reader = new InMemoryModelReader(mdl);
                ModelParser<Vector, Double, ?> parser = new IgniteModelParser<>();
                try (Model<Vector, Future<Double>> infMdl = new IgniteDistributedModelBuilder(ignite, 4, 4)
                    .build(reader, parser)) {
                    System.out.println(">>> Inference model is ready.");

                    System.out.println(">>> ---------------------------------");
                    System.out.println(">>> | Prediction\t| Ground Truth\t|");
                    System.out.println(">>> ---------------------------------");

                    try (QueryCursor<Cache.Entry<Integer, Vector>> observations = dataCache.query(new ScanQuery<>())) {
                        for (Cache.Entry<Integer, Vector> observation : observations) {
                            Vector val = observation.getValue();
                            Vector inputs = val.copyOfRange(1, val.size());
                            double groundTruth = val.get(0);

                            double prediction = infMdl.predict(inputs).get();

                            System.out.printf(">>> | %.4f\t\t| %.4f\t\t|\n", prediction, groundTruth);
                        }
                    }
                }

                System.out.println(">>> ---------------------------------");

                System.out.println(">>> Linear regression model over cache based dataset usage example completed.");
            }
            finally {
                if (dataCache != null)
                    dataCache.destroy();
            }
        }
        finally {
            System.out.flush();
        }
    }
}
