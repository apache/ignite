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

package org.apache.ignite.yardstick.ml;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.decompositions.SingularValueDecomposition;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkUtils;

/**
 * Ignite benchmark that performs ML Grid operations.
 */
@SuppressWarnings("unused")
public class IgniteSingularValueDecompositionBenchmark extends IgniteAbstractBenchmark {
    /** */
    private static AtomicBoolean startLogged = new AtomicBoolean(false);

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        if (!startLogged.getAndSet(true))
            BenchmarkUtils.println("Starting " + this.getClass().getSimpleName());

        runSingularValueDecomposition();

        return true;
    }

    /**
     * Based on SingularValueDecompositionTest#basicTest.
     */
    private void runSingularValueDecomposition() {
        Matrix m = new DenseLocalOnHeapMatrix(new DataChanger.Scale().mutate(new double[][] {
            {2.0d, -1.0d, 0.0d},
            {-1.0d, 2.0d, -1.0d},
            {0.0d, -1.0d, 2.0d}
        }));

        SingularValueDecomposition dec = new SingularValueDecomposition(m);

        Matrix s = dec.getS();
        Matrix u = dec.getU();
        Matrix v = dec.getV();

        u.times(s).times(v.transpose());

        dec.destroy();
    }
}
