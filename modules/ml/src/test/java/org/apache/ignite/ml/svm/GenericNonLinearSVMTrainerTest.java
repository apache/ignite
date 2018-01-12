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

package org.apache.ignite.ml.svm;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.Trainer;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.structures.LabeledDataset;
import org.junit.Test;

/**
 * Base class for all linear regression trainers.
 */
public class GenericNonLinearSVMTrainerTest extends BaseSVMTest {
    /** */
    private final Trainer<SVMLinearClassificationModel, LabeledDataset> trainer;

    /** */
    private LabeledDataset dataset;

    /** */
    private int size;

    /** */
    private final double precision;

    /** */
    public GenericNonLinearSVMTrainerTest(
        Trainer<SVMLinearClassificationModel, LabeledDataset> trainer,
        LabeledDataset dataset,
        int size,
        double precision) {
        super();
        this.trainer = trainer;
        this.dataset = dataset;
        this.precision = precision;
        this.size = size;
    }

    /**
     * Test trainer on classification model y^2 = 100 - x^2. [Circle with radius 10].
     */
    @Test
    public void testTrainWithTheLinearlyUnseparableCase() {

        ThreadLocalRandom rndX = ThreadLocalRandom.current();
        ThreadLocalRandom rndY = ThreadLocalRandom.current();
        for (int i = 0; i < size; i++) {
            // to emulate half of points in class +1 and half of points in class -1
            double x = rndX.nextDouble(-12.5, 12.5);
            double y = rndY.nextDouble(-12.5, 12.5);
            dataset.features(i).set(0, x);
            dataset.features(i).set(1, y);
            double lb =  y*y + x*x > 100 ? -1 : 1;
            dataset.setLabel(i, lb);
        }

        SVMLinearClassificationModel mdl = trainer.train(dataset);
        System.out.println(mdl);

        TestUtils.assertEquals(-1, mdl.apply(new DenseLocalOnHeapVector(new double[] {8, 9})), precision);
        TestUtils.assertEquals(1, mdl.apply(new DenseLocalOnHeapVector(new double[] {7, 7})), precision);
        TestUtils.assertEquals(1, mdl.apply(new DenseLocalOnHeapVector(new double[] {-7, -7})), precision);
        TestUtils.assertEquals(1, mdl.apply(new DenseLocalOnHeapVector(new double[] {-9.99, 0})), precision);
        TestUtils.assertEquals(-1, mdl.apply(new DenseLocalOnHeapVector(new double[] {100, 10})), precision);
    }
}
