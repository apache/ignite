/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.ml.tree.impurity.util;

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
            TestImpurityMeasure.asTestImpurityMeasures(1, 2, 3)
        );

        StepFunction<TestImpurityMeasure> b = new StepFunction<>(
            new double[]{0, 2, 4},
            TestImpurityMeasure.asTestImpurityMeasures(1, 2, 3)
        );

        StepFunction<TestImpurityMeasure> c = a.add(b);

        assertArrayEquals(new double[]{0, 1, 2, 3, 4, 5}, c.getX(), 1e-10);
        assertArrayEquals(
            TestImpurityMeasure.asTestImpurityMeasures(1, 2, 3, 4, 5, 6),
            c.getY()
        );
    }

    /** */
    @Test
    public void testAddDecreasingFunctions() {
        StepFunction<TestImpurityMeasure> a = new StepFunction<>(
            new double[]{1, 3, 5},
            TestImpurityMeasure.asTestImpurityMeasures(3, 2, 1)
        );

        StepFunction<TestImpurityMeasure> b = new StepFunction<>(
            new double[]{0, 2, 4},
            TestImpurityMeasure.asTestImpurityMeasures(3, 2, 1)
        );

        StepFunction<TestImpurityMeasure> c = a.add(b);

        assertArrayEquals(new double[]{0, 1, 2, 3, 4, 5}, c.getX(), 1e-10);
        assertArrayEquals(
            TestImpurityMeasure.asTestImpurityMeasures(3, 6, 5, 4, 3, 2),
            c.getY()
        );
    }
}
