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

package org.apache.ignite.ml.math.primitives.vector;

import java.util.Arrays;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.ignite.ml.math.functions.Functions;
import org.junit.Test;

import static java.util.function.DoubleUnaryOperator.identity;
import static org.junit.Assert.assertTrue;

/** See also: {@link AbstractVectorTest} and {@link VectorToMatrixTest}. */
public class VectorFoldMapTest {
    /** */
    @Test
    public void mapVectorTest() {
        operationVectorTest((operand1, operand2) -> operand1 + operand2, (Vector v1, Vector v2) -> v1.map(v2, Functions.PLUS));
    }

    /** */
    @Test
    public void mapDoubleFunctionTest() {
        consumeSampleVectors((v, desc) -> operatorTest(v, desc,
            (vec) -> vec.map(Functions.INV), (val) -> 1.0 / val));
    }

    /** */
    @Test
    public void mapBiFunctionTest() {
        consumeSampleVectors((v, desc) -> operatorTest(v, desc,
            (vec) -> vec.map(Functions.PLUS, 1.0), (val) -> 1.0 + val));
    }

    /** */
    @Test
    public void foldMapTest() {
        toDoubleTest(
            ref -> Arrays.stream(ref).map(identity()).sum(),
            (v) -> v.foldMap(Functions.PLUS, Functions.IDENTITY, 0.0));
    }

    /** */
    @Test
    public void foldMapVectorTest() {
        toDoubleTest(
            ref -> 2.0 * Arrays.stream(ref).sum(),
            (v) -> v.foldMap(v, Functions.PLUS, Functions.PLUS, 0.0));

    }

    /** */
    private void operatorTest(Vector v, String desc, Function<Vector, Vector> op, Function<Double, Double> refOp) {
        final int size = v.size();
        final double[] ref = new double[size];

        VectorImplementationsTest.ElementsChecker checker = new VectorImplementationsTest.ElementsChecker(v, ref, desc);

        Vector actual = op.apply(v);

        for (int idx = 0; idx < size; idx++)
            ref[idx] = refOp.apply(ref[idx]);

        checker.assertCloseEnough(actual, ref);
    }

    /** */
    private void toDoubleTest(Function<double[], Double> calcRef, Function<Vector, Double> calcVec) {
        consumeSampleVectors((v, desc) -> {
            final int size = v.size();
            final double[] ref = new double[size];

            new VectorImplementationsTest.ElementsChecker(v, ref, desc); // IMPL NOTE this initialises vector and reference array

            final VectorImplementationsTest.Metric metric = new VectorImplementationsTest.Metric(calcRef.apply(ref), calcVec.apply(v));

            assertTrue("Not close enough at " + desc
                + ", " + metric, metric.closeEnough());
        });
    }

    /** */
    private void operationVectorTest(BiFunction<Double, Double, Double> operation,
        BiFunction<Vector, Vector, Vector> vecOperation) {
        consumeSampleVectors((v, desc) -> {
            // TODO: IGNITE-5723, find out if more elaborate testing scenario is needed or it's okay as is.
            final int size = v.size();
            final double[] ref = new double[size];

            final VectorImplementationsTest.ElementsChecker checker = new VectorImplementationsTest.ElementsChecker(v, ref, desc);
            final Vector operand = v.copy();

            for (int idx = 0; idx < size; idx++)
                ref[idx] = operation.apply(ref[idx], ref[idx]);

            checker.assertCloseEnough(vecOperation.apply(v, operand), ref);
        });
    }

    /** */
    private void consumeSampleVectors(BiConsumer<Vector, String> consumer) {
        new VectorImplementationsFixtures().consumeSampleVectors(null, consumer);
    }
}
