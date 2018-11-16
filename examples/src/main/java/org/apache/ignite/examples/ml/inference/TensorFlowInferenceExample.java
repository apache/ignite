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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.ml.inference.InfModel;
import org.apache.ignite.ml.inference.builder.IgniteDistributedInfModelBuilder;
import org.apache.ignite.ml.inference.builder.SingleInfModelBuilder;
import org.apache.ignite.ml.inference.builder.ThreadedInfModelBuilder;
import org.apache.ignite.ml.inference.parser.InfModelParser;
import org.apache.ignite.ml.inference.parser.TensorFlowSavedModelInfModelParser;
import org.apache.ignite.ml.inference.reader.DirectoryInfModelReader;
import org.apache.ignite.ml.inference.reader.InfModelReader;
import org.apache.ignite.ml.util.MnistUtils;
import org.tensorflow.Tensor;

public class TensorFlowInferenceExample {

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            InfModelReader reader = new DirectoryInfModelReader("/home/gridgain/model/1541682474/");

            InfModelParser<double[], Long> parser = new TensorFlowSavedModelInfModelParser<double[], Long>("serve")

                .withInput("Placeholder", doubles -> {
                    float[][][] reshaped = new float[1][28][28];
                    for (int i = 0; i < doubles.length; i++)
                        reshaped[0][i / 28][i % 28] = (float)doubles[i];
                    return Tensor.create(reshaped);
                })

                .withOutput(Collections.singletonList("ArgMax"), collectedTensors -> {
                    return collectedTensors.get("ArgMax").copyTo(new long[1])[0];
                });

            List<MnistUtils.MnistLabeledImage> images = MnistUtils.mnistAsList(
                "/home/gridgain/test-tensorflow-server/src/main/resources/train-images-idx3-ubyte",
                "/home/gridgain/test-tensorflow-server/src/main/resources/train-labels-idx1-ubyte",
                new Random(0),
                10000
            );

            // -------------------- TEST LOCAL MODEL --------------------
            long t1 = System.currentTimeMillis();

            try (InfModel<double[], Long> locMdl = new SingleInfModelBuilder().build(reader, parser)) {
                for (MnistUtils.MnistLabeledImage image : images)
                    locMdl.predict(image.getPixels());
            }

            long t2 = System.currentTimeMillis();
            System.out.println("Local model time: " + (t2 - t1) / 1000 + " s");
            // -------------------- TEST LOCAL MODEL END ----------------

            // -------------------- TEST DISTRIBUTED MODEL --------------
            try (InfModel<double[], Future<Long>> distributedMdl = new IgniteDistributedInfModelBuilder(ignite, 4)
                         .build(reader, parser)) {
                List<Future<?>> futures = new ArrayList<>(images.size());
                for (MnistUtils.MnistLabeledImage image : images)
                    futures.add(distributedMdl.predict(image.getPixels()));
                for (Future<?> f : futures)
                    f.get();
            }

            long t3 = System.currentTimeMillis();
            System.out.println("Distributed model time: " + (t3 - t2) / 1000 + " s");
            // -------------------- TEST DISTRIBUTED MODEL END ----------

            // -------------------- TEST THREADED MODEL -----------------
            try (InfModel<double[], Future<Long>> threadedMdl = new ThreadedInfModelBuilder(8)
                .build(reader, parser)) {
                List<Future<?>> futures = new ArrayList<>(images.size());
                for (MnistUtils.MnistLabeledImage image : images)
                    futures.add(threadedMdl.predict(image.getPixels()));
                for (Future<?> f : futures)
                    f.get();
            }

            long t4 = System.currentTimeMillis();
            System.out.println("Threaded model time: " + (t4 - t3) / 1000 + " s");
            // -------------------- TEST THREADED MODEL END -------------
        }
    }
}
