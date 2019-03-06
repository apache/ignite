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

package org.apache.ignite.ml.math.primitives.matrix;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import org.apache.ignite.ml.math.primitives.matrix.impl.DenseMatrix;
import org.apache.ignite.ml.math.primitives.matrix.impl.SparseMatrix;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Attribute tests for matrices.
 */
public class MatrixAttributeTest {
    /** */
    private final List<MatrixAttributeTest.AttrCfg> attrCfgs = Arrays.asList(
        new AttrCfg("isDense", Matrix::isDense,
            DenseMatrix.class),
        new AttrCfg("isArrayBased", Matrix::isArrayBased, DenseMatrix.class),
        new AttrCfg("isDistributed", Matrix::isDistributed),
        new AttrCfg("isRandomAccess", Matrix::isRandomAccess, DenseMatrix.class, SparseMatrix.class)
    );

    /** */
    private final List<MatrixAttributeTest.Specification> specFixture = Arrays.asList(
        new Specification(new DenseMatrix(1, 1)),
        new Specification(new SparseMatrix(1, 1))
    );

    /** */
    @Test
    public void isDenseTest() {
        assertAttribute("isDense");
    }

    /** */
    @Test
    public void isArrayBasedTest() {
        assertAttribute("isArrayBased");
    }

    /** */
    @Test
    public void isRandomAccessTest() {
        assertAttribute("isRandomAccess");
    }

    /** */
    @Test
    public void isDistributedTest() {
        assertAttribute("isDistributed");
    }

    /** */
    private void assertAttribute(String name) {
        final MatrixAttributeTest.AttrCfg attr = attrCfg(name);

        for (MatrixAttributeTest.Specification spec : specFixture)
            spec.verify(attr);
    }

    /** */
    private MatrixAttributeTest.AttrCfg attrCfg(String name) {
        for (MatrixAttributeTest.AttrCfg attr : attrCfgs)
            if (attr.name.equals(name))
                return attr;

        throw new IllegalArgumentException("Undefined attribute " + name);
    }

    /** See http://en.wikipedia.org/wiki/Specification_pattern */
    private static class Specification {
        /** */
        private final Matrix m;
        /** */
        private final Class<? extends Matrix> underlyingType;
        /** */
        private final List<String> attrsFromUnderlying;
        /** */
        final String desc;

        /** */
        Specification(Matrix m, Class<? extends Matrix> underlyingType, String... attrsFromUnderlying) {
            this.m = m;
            this.underlyingType = underlyingType;
            this.attrsFromUnderlying = Arrays.asList(attrsFromUnderlying);
            final Class<? extends Matrix> clazz = m.getClass();
            desc = clazz.getSimpleName() + (clazz.equals(underlyingType)
                ? "" : " (underlying type " + underlyingType.getSimpleName() + ")");
        }

        /** */
        Specification(Matrix m) {
            this(m, m.getClass());
        }

        /** */
        void verify(MatrixAttributeTest.AttrCfg attr) {
            final boolean obtained = attr.obtain.apply(m);

            final Class<? extends Matrix> typeToCheck
                = attrsFromUnderlying.contains(attr.name) ? underlyingType : m.getClass();

            final boolean exp = attr.trueInTypes.contains(typeToCheck);

            assertEquals("Unexpected " + attr.name + " value for " + desc, exp, obtained);
        }
    }

    /** */
    private static class AttrCfg {
        /** */
        final String name;
        /** */
        final Function<Matrix, Boolean> obtain;
        /** */
        final List<Class> trueInTypes;

        /** */
        AttrCfg(String name, Function<Matrix, Boolean> obtain, Class... trueInTypes) {
            this.name = name;
            this.obtain = obtain;
            this.trueInTypes = Arrays.asList(trueInTypes);
        }
    }
}
