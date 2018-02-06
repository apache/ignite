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

package org.apache.ignite.ml.svm.binary;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.Trainer;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.structures.LabeledDataset;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.svm.BaseSVMTest;
import org.apache.ignite.ml.svm.SVMLinearBinaryClassificationModel;
import org.junit.Test;

/**
 * Base class for all linear regression trainers.
 */
public class GenericLinearSVMBinaryClassificationTrainerTest extends BaseSVMTest {
    /** Fixed size of Dataset. */
    private static final int AMOUNT_OF_OBSERVATIONS = 100;

    /** Fixed size of columns in Dataset. */
    private static final int AMOUNT_OF_FEATURES = 2;

    /** */
    private final Trainer<SVMLinearBinaryClassificationModel, LabeledDataset> trainer;

    /** */
    private boolean isDistributed;

    /** */
    private final double precision;

    /** */
    GenericLinearSVMBinaryClassificationTrainerTest(
        Trainer<SVMLinearBinaryClassificationModel, LabeledDataset> trainer,
        boolean isDistributed,
        double precision) {
        super();
        this.trainer = trainer;
        this.precision = precision;
        this.isDistributed = isDistributed;
    }

    /**
     * Test trainer on classification model y = x.
     */
    @Test
    public void testTrainWithTheLinearlySeparableCase() {
        if (isDistributed)
            IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        LabeledDataset dataset = new LabeledDataset<Double, LabeledVector>(AMOUNT_OF_OBSERVATIONS, AMOUNT_OF_FEATURES, isDistributed);

        ThreadLocalRandom rndX = ThreadLocalRandom.current();
        ThreadLocalRandom rndY = ThreadLocalRandom.current();
        for (int i = 0; i < AMOUNT_OF_OBSERVATIONS; i++) {
            double x = rndX.nextDouble(-1000, 1000);
            double y = rndY.nextDouble(-1000, 1000);
            dataset.features(i).set(0, x);
            dataset.features(i).set(1, y);
            double lb = y - x > 0 ? 1 : -1;
            dataset.setLabel(i, lb);
        }

        SVMLinearBinaryClassificationModel mdl = trainer.train(dataset);

        TestUtils.assertEquals(-1, mdl.apply(new DenseLocalOnHeapVector(new double[] {100, 10})), precision);
        TestUtils.assertEquals(1, mdl.apply(new DenseLocalOnHeapVector(new double[] {10, 100})), precision);
    }

    /**
     * Test trainer on classification model y = x. Amount of generated points is increased 10 times.
     */
    @Test
    public void testTrainWithTheLinearlySeparableCase10() {
        if (isDistributed)
            IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        LabeledDataset dataset = new LabeledDataset<Double, LabeledVector>(AMOUNT_OF_OBSERVATIONS * 10, AMOUNT_OF_FEATURES, isDistributed);

        ThreadLocalRandom rndX = ThreadLocalRandom.current();
        ThreadLocalRandom rndY = ThreadLocalRandom.current();
        for (int i = 0; i < AMOUNT_OF_OBSERVATIONS * 10; i++) {
            double x = rndX.nextDouble(-1000, 1000);
            double y = rndY.nextDouble(-1000, 1000);
            dataset.features(i).set(0, x);
            dataset.features(i).set(1, y);
            double lb = y - x > 0 ? 1 : -1;
            dataset.setLabel(i, lb);
        }

        SVMLinearBinaryClassificationModel mdl = trainer.train(dataset);

        TestUtils.assertEquals(-1, mdl.apply(new DenseLocalOnHeapVector(new double[] {100, 10})), precision);
        TestUtils.assertEquals(1, mdl.apply(new DenseLocalOnHeapVector(new double[] {10, 100})), precision);
    }

    /**
     * Test trainer on classification model y = x. Amount of generated points is increased 100 times.
     */
    @Test
    public void testTrainWithTheLinearlySeparableCase100() {
        if (isDistributed)
            IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        LabeledDataset dataset = new LabeledDataset<Double, LabeledVector>(AMOUNT_OF_OBSERVATIONS * 100, AMOUNT_OF_FEATURES, isDistributed);

        ThreadLocalRandom rndX = ThreadLocalRandom.current();
        ThreadLocalRandom rndY = ThreadLocalRandom.current();
        for (int i = 0; i < AMOUNT_OF_OBSERVATIONS * 100; i++) {
            double x = rndX.nextDouble(-1000, 1000);
            double y = rndY.nextDouble(-1000, 1000);
            dataset.features(i).set(0, x);
            dataset.features(i).set(1, y);
            double lb = y - x > 0 ? 1 : -1;
            dataset.setLabel(i, lb);
        }

        SVMLinearBinaryClassificationModel mdl = trainer.train(dataset);

        TestUtils.assertEquals(-1, mdl.apply(new DenseLocalOnHeapVector(new double[] {100, 10})), precision);
        TestUtils.assertEquals(1, mdl.apply(new DenseLocalOnHeapVector(new double[] {10, 100})), precision);
    }
}
