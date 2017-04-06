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

package org.apache.ignite.examples.java8.math.decompositions;

import org.apache.ignite.math.Matrix;
import org.apache.ignite.math.Tracer;
import org.apache.ignite.math.decompositions.CholeskyDecomposition;
import org.apache.ignite.math.decompositions.EigenDecomposition;
import org.apache.ignite.math.decompositions.LUDecomposition;
import org.apache.ignite.math.decompositions.QRDecomposition;
import org.apache.ignite.math.decompositions.SingularValueDecomposition;
import org.apache.ignite.math.impls.matrix.DenseLocalOnHeapMatrix;

public class VarExamples {
    public static void main(String[] args) {
        Matrix m = new DenseLocalOnHeapMatrix(new double[][] {
            {2.0d, 0.0d},
            {0.0d, 2.0d}
        });
        Tracer.showAscii(m.inverse());
        new CholeskyDecomposition(m).destroy();
        new EigenDecomposition(m).destroy();
        new QRDecomposition(m).destroy();
        new SingularValueDecomposition(m).destroy();
        new LUDecomposition(m).destroy();
    }
}
