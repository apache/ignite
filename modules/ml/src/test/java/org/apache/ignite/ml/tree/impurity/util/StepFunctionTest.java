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

package org.apache.ignite.ml.tree.impurity.util;

import java.util.Objects;
import org.apache.ignite.ml.tree.impurity.ImpurityMeasure;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for {@link StepFunction}.
 */
public class StepFunctionTest {
    /** */
    @Test
    public void testAddIncreasingFunctions() {
        StepFunction<TestImpurityMeasure> a = new StepFunction<>(
            new double[]{1, 3, 5},
            asTestImpurityMeasures(new double[]{1, 2, 3})
        );

        StepFunction<TestImpurityMeasure> b = new StepFunction<>(
            new double[]{0, 2, 4},
            asTestImpurityMeasures(new double[]{1, 2, 3})
        );

        StepFunction<TestImpurityMeasure> c = a.add(b);

        assertArrayEquals(new double[]{0, 1, 2, 3, 4, 5}, c.getX(), 1e-10);
        assertArrayEquals(
            asTestImpurityMeasures(new double[]{1, 2, 3, 4, 5, 6}),
            c.getY()
        );
    }

    /** */
    @Test
    public void testAddDecreasingFunctions() {
        StepFunction<TestImpurityMeasure> a = new StepFunction<>(
            new double[]{1, 3, 5},
            asTestImpurityMeasures(new double[]{3, 2, 1})
        );

        StepFunction<TestImpurityMeasure> b = new StepFunction<>(
            new double[]{0, 2, 4},
            asTestImpurityMeasures(new double[]{3, 2, 1})
        );

        StepFunction<TestImpurityMeasure> c = a.add(b);

        assertArrayEquals(new double[]{0, 1, 2, 3, 4, 5}, c.getX(), 1e-10);
        assertArrayEquals(
            asTestImpurityMeasures(new double[]{3, 6, 5, 4, 3, 2}),
            c.getY()
        );
    }

    /** */
    private TestImpurityMeasure[] asTestImpurityMeasures(double[] impurity) {
        TestImpurityMeasure[] res = new TestImpurityMeasure[impurity.length];

        for (int i = 0; i < impurity.length; i++)
            res[i] = new TestImpurityMeasure(impurity[i]);

        return res;
    }

    /**
     * Utils class used as impurity measure in tests.
     */
    private static class TestImpurityMeasure implements ImpurityMeasure<TestImpurityMeasure> {
        /** Impurity. */
        private final double impurity;

        /**
         * Constructs a new instance of test impurity measure.
         *
         * @param impurity Impurity.
         */
        TestImpurityMeasure(double impurity) {
            this.impurity = impurity;
        }

        /** {@inheritDoc} */
        @Override public double impurity() {
            return impurity;
        }

        /** {@inheritDoc} */
        @Override public TestImpurityMeasure add(TestImpurityMeasure measure) {
            return new TestImpurityMeasure(impurity + measure.impurity);
        }

        /** {@inheritDoc} */
        @Override public TestImpurityMeasure subtract(TestImpurityMeasure measure) {
            return new TestImpurityMeasure(impurity - measure.impurity);
        }

        /** */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            TestImpurityMeasure measure = (TestImpurityMeasure)o;

            return Double.compare(measure.impurity, impurity) == 0;
        }

        /** */
        @Override public int hashCode() {

            return Objects.hash(impurity);
        }
    }
}
