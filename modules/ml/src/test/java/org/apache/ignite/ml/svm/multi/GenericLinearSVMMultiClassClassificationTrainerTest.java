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

import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.Trainer;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.structures.LabeledDataset;
import org.apache.ignite.ml.svm.BaseSVMTest;
import org.apache.ignite.ml.svm.SVMLinearMultiClassClassificationModel;
import org.junit.Test;

/**
 * Base class for all linear regression trainers.
 */
public class GenericLinearSVMMultiClassClassificationTrainerTest extends BaseSVMTest {
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
        double[][] mtx =
            new double[][] {
                {-10.0, 12.0},
                {-5.0, 14.0},
                {-3.0, 18.0},
                {13.0, -1.0},
                {10.0, -2.0},
                {15.0, -3.0}};
        double[] lbs = new double[] {1.0, 1.0, 1.0, 2.0, 2.0, 2.0};
        LabeledDataset dataset = new LabeledDataset(mtx, lbs, null, isDistributed);


        SVMLinearMultiClassClassificationModel mdl = trainer.train(dataset);
        TestUtils.assertEquals(1.0, mdl.apply(new DenseLocalOnHeapVector(new double[] {-2.0, 15})), precision);
        TestUtils.assertEquals(2.0, mdl.apply(new DenseLocalOnHeapVector(new double[] {12, -5})), precision);
    }
}
