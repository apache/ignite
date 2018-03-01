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

import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.regressions.linear.LinearRegressionSGDTrainer;
import org.apache.ignite.ml.svm.SVMLinearMultiClassClassificationTrainer;

/**
 * Tests for {@link LinearRegressionSGDTrainer} on {@link DenseLocalOnHeapMatrix}.
 */
public class LocalLinearSVMMultiClassClassificationTrainerTest extends GenericLinearSVMMultiClassClassificationTrainerTest {
    /** */
    public LocalLinearSVMMultiClassClassificationTrainerTest() {
        super(
            new SVMLinearMultiClassClassificationTrainer()
                .withLambda(0.2)
                .withAmountOfIterations(10)
                .withAmountOfLocIterations(20),
            false,
            1e-2);
    }
}
