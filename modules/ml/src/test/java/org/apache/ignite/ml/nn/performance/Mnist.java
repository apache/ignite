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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Tracer;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.VectorUtils;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.nn.Activators;
import org.apache.ignite.ml.nn.LossFunctions;
import org.apache.ignite.ml.nn.MultilayerPerceptron;
import org.apache.ignite.ml.nn.SimpleMLPLocalBatchTrainerInput;
import org.apache.ignite.ml.nn.architecture.MLPArchitecture;
import org.apache.ignite.ml.nn.trainers.local.MLPLocalBatchTrainer;
import org.apache.ignite.ml.nn.updaters.RPropUpdater;
import org.apache.ignite.ml.trees.performance.ColumnDecisionTreeTrainerBenchmark;
import org.apache.ignite.ml.util.MnistUtils;
import org.junit.Test;

import static org.apache.ignite.ml.math.VectorUtils.num2Vec;

/**
 * Various benchmarks for hand runs.
 */
public class Mnist {
    /** Name of the property specifying path to training set images. */
    private static final String PROP_TRAINING_IMAGES = "mnist.training.images";

    /** Name of property specifying path to training set labels. */
    private static final String PROP_TRAINING_LABELS = "mnist.training.labels";

    /** Name of property specifying path to test set images. */
    private static final String PROP_TEST_IMAGES = "mnist.test.images";

    /** Name of property specifying path to test set labels. */
    private static final String PROP_TEST_LABELS = "mnist.test.labels";

    /**
     * Run decision tree classifier on MNIST using bi-indexed cache as a storage for dataset.
     * To run this test rename this method so it starts from 'test'.
     *
     * @throws IOException In case of loading MNIST dataset errors.
     */
    @Test
    public void tstMNIST() throws IOException {
        int samplesCntCnt = 60_000;
        int featCnt = 28 * 28;
        int hiddenNeuronsCnt = 100;

        Properties props = loadMNISTProperties();

        Stream<DenseLocalOnHeapVector> trainingMnistStream = MnistUtils.mnist(props.getProperty(PROP_TRAINING_IMAGES),
            props.getProperty(PROP_TRAINING_LABELS), new Random(123L), samplesCntCnt);

        Stream<DenseLocalOnHeapVector> testMnistStream = MnistUtils.mnist(props.getProperty(PROP_TEST_IMAGES),
            props.getProperty(PROP_TEST_LABELS), new Random(123L), 10_000);

        IgniteBiTuple<Matrix, Matrix> ds = createDataset(trainingMnistStream, samplesCntCnt, featCnt);
        IgniteBiTuple<Matrix, Matrix> testDs = createDataset(testMnistStream, 10000, featCnt);

        MLPArchitecture conf = new MLPArchitecture(featCnt).
            withAddedLayer(hiddenNeuronsCnt, true, Activators.SIGMOID).
            withAddedLayer(10, false, Activators.SIGMOID);

        SimpleMLPLocalBatchTrainerInput input = new SimpleMLPLocalBatchTrainerInput(conf,
            new Random(),
            ds.get1(),
            ds.get2(),
            2000);

        MultilayerPerceptron mdl = new MLPLocalBatchTrainer<>(LossFunctions.MSE,
            () -> new RPropUpdater(0.1, 1.2, 0.5),
            1E-7,
            200).
            train(input);

        X.println("Training started");
        long before = System.currentTimeMillis();

        X.println("Training finished in " + (System.currentTimeMillis() - before));

        Vector predicted = mdl.apply(testDs.get1()).foldColumns(VectorUtils::vec2Num);
        Vector truth = testDs.get2().foldColumns(VectorUtils::vec2Num);

        Tracer.showAscii(truth);
        Tracer.showAscii(predicted);
    }

    /** */
    private IgniteBiTuple<Matrix, Matrix> createDataset(Stream<DenseLocalOnHeapVector> s, int samplesCnt, int featCnt) {
        Matrix vectors = new DenseLocalOnHeapMatrix(featCnt, samplesCnt);
        Matrix labels = new DenseLocalOnHeapMatrix(10, samplesCnt);
        List<DenseLocalOnHeapVector> sc = s.collect(Collectors.toList());

        for (int i = 0; i < samplesCnt; i++) {
            DenseLocalOnHeapVector v = sc.get(i);
            vectors.assignColumn(i, v.viewPart(0, featCnt));
            labels.assignColumn(i, num2Vec((int)v.getX(featCnt), 10));
        }

        return new IgniteBiTuple<>(vectors, labels);
    }

    /** Load properties for MNIST tests. */
    private static Properties loadMNISTProperties() throws IOException {
        Properties res = new Properties();

        InputStream is = ColumnDecisionTreeTrainerBenchmark.class.getClassLoader().getResourceAsStream("manualrun/trees/columntrees.manualrun.properties");

        res.load(is);

        return res;
    }
}
