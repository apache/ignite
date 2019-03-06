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

import java.util.Objects;
import org.apache.ignite.ml.tree.impurity.ImpurityMeasure;

/**
 * Utils class used as impurity measure in tests.
 */
class TestImpurityMeasure implements ImpurityMeasure<TestImpurityMeasure> {
    /** */
    private static final long serialVersionUID = 2414020770162797847L;

    /** Impurity. */
    private final double impurity;

    /**
     * Constructs a new instance of test impurity measure.
     *
     * @param impurity Impurity.
     */
    private TestImpurityMeasure(double impurity) {
        this.impurity = impurity;
    }

    /**
     * Convert doubles to array of test impurity measures.
     *
     * @param impurity Impurity as array of doubles.
     * @return Test impurity measure objects as array.
     */
    static TestImpurityMeasure[] asTestImpurityMeasures(double... impurity) {
        TestImpurityMeasure[] res = new TestImpurityMeasure[impurity.length];

        for (int i = 0; i < impurity.length; i++)
            res[i] = new TestImpurityMeasure(impurity[i]);

        return res;
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
