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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.util.IgniteUtils;
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

/**
 * This example demonstrates how to: load TensorFlow model into Java, make inference using this model in one thread,
 * multiple threads and in distributed environment using Apache Ignite.
 */
public class TensorFlowInferenceExample {
    /** Path to the directory with saved TensorFlow model. */
    private static final String MODEL_PATH = "examples/src/main/resources/ml/mnist_tf_model";

    /** Path to the MNIST images data. */
    private static final String MNIST_IMG_PATH = "org/apache/ignite/examples/ml/util/datasets/t10k-images-idx3-ubyte";

    /** Path to the MNIST labels data. */
    private static final String MNIST_LBL_PATH = "org/apache/ignite/examples/ml/util/datasets/t10k-labels-idx1-ubyte";

    /** Run example. */
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            File mdlRsrc = IgniteUtils.resolveIgnitePath(MODEL_PATH);
            if (mdlRsrc == null)
                throw new IllegalArgumentException("Resource not found [resource_path=" + MODEL_PATH + "]");

            InfModelReader reader = new DirectoryInfModelReader(mdlRsrc.getPath());

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

            List<MnistUtils.MnistLabeledImage> images = MnistUtils.mnistAsListFromResource(
                MNIST_IMG_PATH,
                MNIST_LBL_PATH,
                new Random(0),
                10000
            );

            System.out.println("Testing local model...");
            long t1 = System.currentTimeMillis();

            try (InfModel<double[], Long> locMdl = new SingleInfModelBuilder().build(reader, parser)) {
                for (MnistUtils.MnistLabeledImage image : images)
                    locMdl.predict(image.getPixels());
            }

            long t2 = System.currentTimeMillis();
            System.out.println("Local model time: " + (t2 - t1) / 1000 + " s");

            System.out.println("Testing distributed model...");
            try (InfModel<double[], Future<Long>> distributedMdl = new IgniteDistributedInfModelBuilder(ignite, 4, 4)
                         .build(reader, parser)) {
                List<Future<?>> futures = new ArrayList<>(images.size());
                for (MnistUtils.MnistLabeledImage image : images)
                    futures.add(distributedMdl.predict(image.getPixels()));
                for (Future<?> f : futures)
                    f.get();
            }

            long t3 = System.currentTimeMillis();
            System.out.println("Distributed model time: " + (t3 - t2) / 1000 + " s");

            System.out.println("Testing threaded model...");
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
        }
    }
}
