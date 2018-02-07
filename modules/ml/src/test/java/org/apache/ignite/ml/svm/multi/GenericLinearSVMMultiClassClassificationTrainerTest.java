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

package org.apache.ignite.ml.svm.multi;

import java.awt.*;
import java.util.concurrent.ThreadLocalRandom;
import javax.swing.*;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.Trainer;
import org.apache.ignite.ml.math.Tracer;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.structures.LabeledDataset;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.svm.BaseSVMTest;
import org.apache.ignite.ml.svm.SVMLinearBinaryClassificationModel;
import org.apache.ignite.ml.svm.SVMLinearMultiClassClassificationModel;
import org.junit.Test;

/**
 * Base class for all linear regression trainers.
 */
public class GenericLinearSVMMultiClassClassificationTrainerTest extends BaseSVMTest {
    /** Fixed size of Dataset. */
    private static final int AMOUNT_OF_OBSERVATIONS = 1000;

    /** Fixed size of columns in Dataset. */
    private static final int AMOUNT_OF_FEATURES = 2;

    /** */
    private final Trainer<SVMLinearMultiClassClassificationModel, LabeledDataset> trainer;

    /** */
    private boolean isDistributed;

    /** */
    private final double precision;

    /** */
    GenericLinearSVMMultiClassClassificationTrainerTest(
        Trainer<SVMLinearMultiClassClassificationModel, LabeledDataset> trainer,
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
            if(y > 10) dataset.setLabel(i, 1.0);
            else if(y < x) dataset.setLabel(i, 2.0);
            else dataset.setLabel(i, 3.0);
        }
        JFrame frame = new JFrame();
        frame.setLocationRelativeTo(null);
        frame.setVisible(true);


        SVMLinearMultiClassClassificationModel mdl = trainer.train(dataset);
        TestUtils.assertEquals(1.0, mdl.apply(new DenseLocalOnHeapVector(new double[] {0, 100})), precision);
        TestUtils.assertEquals(2.0, mdl.apply(new DenseLocalOnHeapVector(new double[] {3, -10})), precision);
        TestUtils.assertEquals(3.0, mdl.apply(new DenseLocalOnHeapVector(new double[] {-100, -10})), precision);
    }
}
