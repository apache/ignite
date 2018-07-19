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

package org.apache.ignite.ml.nn.performance;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Stream;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.util.MnistUtils;

/** */
public class MnistMLPTestUtil {
    /** Name of the property specifying path to training set images. */
    private static final String PROP_TRAINING_IMAGES = "mnist.training.images";

    /** Name of property specifying path to training set labels. */
    private static final String PROP_TRAINING_LABELS = "mnist.training.labels";

    /** Name of property specifying path to test set images. */
    private static final String PROP_TEST_IMAGES = "mnist.test.images";

    /** Name of property specifying path to test set labels. */
    private static final String PROP_TEST_LABELS = "mnist.test.labels";

    /** */
    static IgniteBiTuple<Stream<DenseVector>, Stream<DenseVector>> loadMnist(int samplesCnt) throws IOException {
        Properties props = loadMNISTProperties();

        Stream<DenseVector> trainingMnistStream = MnistUtils.mnistAsStream(props.getProperty(PROP_TRAINING_IMAGES),
            props.getProperty(PROP_TRAINING_LABELS), new Random(123L), samplesCnt);

        Stream<DenseVector> testMnistStream = MnistUtils.mnistAsStream(props.getProperty(PROP_TEST_IMAGES),
            props.getProperty(PROP_TEST_LABELS), new Random(123L), 10_000);

        return new IgniteBiTuple<>(trainingMnistStream, testMnistStream);
    }

    /**
     * Loads training set.
     *
     * @param cnt Count of objects.
     * @return List of MNIST images.
     * @throws IOException In case of exception.
     */
    public static List<MnistUtils.MnistLabeledImage> loadTrainingSet(int cnt) throws IOException {
        Properties props = loadMNISTProperties();
        return MnistUtils.mnistAsList(props.getProperty(PROP_TRAINING_IMAGES), props.getProperty(PROP_TRAINING_LABELS), new Random(123L), cnt);
    }

    /**
     * Loads test set.
     *
     * @param cnt Count of objects.
     * @return List of MNIST images.
     * @throws IOException In case of exception.
     */
    public static List<MnistUtils.MnistLabeledImage> loadTestSet(int cnt) throws IOException {
        Properties props = loadMNISTProperties();
        return MnistUtils.mnistAsList(props.getProperty(PROP_TEST_IMAGES), props.getProperty(PROP_TEST_LABELS), new Random(123L), cnt);
    }

    /** Load properties for MNIST tests. */
    private static Properties loadMNISTProperties() throws IOException {
        Properties res = new Properties();

        InputStream is = MnistMLPTestUtil.class.getClassLoader().getResourceAsStream("manualrun/trees/columntrees.manualrun.properties");

        res.load(is);

        return res;
    }
}
