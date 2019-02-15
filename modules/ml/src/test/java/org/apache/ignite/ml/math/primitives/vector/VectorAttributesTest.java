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

package org.apache.ignite.ml.math.primitives.vector;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import org.apache.ignite.ml.math.primitives.matrix.impl.DenseMatrix;
import org.apache.ignite.ml.math.primitives.vector.impl.DelegatingVector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.math.primitives.vector.impl.SparseVector;
import org.apache.ignite.ml.math.primitives.vector.impl.VectorizedViewMatrix;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** */
public class VectorAttributesTest {
    /** */
    private final List<AttrCfg> attrCfgs = Arrays.asList(
        new AttrCfg("isDense", Vector::isDense,
            DenseVector.class),
        new AttrCfg("isArrayBased", Vector::isArrayBased,
            DenseVector.class),
        new AttrCfg("isSequentialAccess", Vector::isSequentialAccess,
            DenseVector.class, SparseLocalVectorSequentialAccess.class),
        new AttrCfg("guidNotNull", v -> v.guid() == null), // IMPL NOTE this is somewhat artificial
        new AttrCfg("isRandomAccess", Vector::isRandomAccess,
            DenseVector.class, SparseLocalVectorSequentialAccess.class, SparseLocalVectorRandomAccess.class),
        new AttrCfg("isDistributed", Vector::isDistributed));

    /** */
    private final List<Specification> specFixture = Arrays.asList(
        new Specification(new DenseVector(1)),
        new Specification(new DelegatingVector(new DenseVector(1)),
            DenseVector.class, "isDense", "isArrayBased", "isSequentialAccess",
            "isRandomAccess", "isDistributed"),
        new Specification(new SparseLocalVectorSequentialAccess(1)),
        new Specification(new SparseLocalVectorRandomAccess(1)),
        new Specification(new VectorizedViewMatrix(new DenseMatrix(1, 1), 0, 0, 1, 1),
            DenseVector.class, "isDense",
            "isRandomAccess", "isDistributed")); // TODO: IGNTIE-5723, find out why "isSequentialAccess" fails here

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
    public void isSequentialAccessTest() {
        assertAttribute("isSequentialAccess");
    }

    /** */
    @Test
    public void guidTest() {
        assertAttribute("guidNotNull");
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
        final AttrCfg attr = attrCfg(name);

        for (Specification spec : specFixture)
            spec.verify(attr);
    }

    /** */
    private AttrCfg attrCfg(String name) {
        for (AttrCfg attr : attrCfgs)
            if (attr.name.equals(name))
                return attr;

        throw new IllegalArgumentException("Undefined attribute " + name);
    }

    /** See http://en.wikipedia.org/wiki/Specification_pattern */
    private static class Specification {
        /** */
        private final Vector v;
        /** */
        private final Class<? extends Vector> underlyingType;
        /** */
        private final List<String> attrsFromUnderlying;
        /** */
        final String desc;

        /** */
        Specification(Vector v, Class<? extends Vector> underlyingType, String... attrsFromUnderlying) {
            this.v = v;
            this.underlyingType = underlyingType;
            this.attrsFromUnderlying = Arrays.asList(attrsFromUnderlying);
            final Class<? extends Vector> clazz = v.getClass();
            desc = clazz.getSimpleName() + (clazz.equals(underlyingType)
                ? "" : " (underlying type " + underlyingType.getSimpleName() + ")");
        }

        /** */
        Specification(Vector v) {
            this(v, v.getClass());
        }

        /** */
        void verify(AttrCfg attr) {
            final boolean obtained = attr.obtain.apply(v);

            final Class<? extends Vector> typeToCheck
                = attrsFromUnderlying.contains(attr.name) ? underlyingType : v.getClass();

            final boolean exp = attr.trueInTypes.contains(typeToCheck);

            assertEquals("Unexpected " + attr.name + " value for " + desc, exp, obtained);
        }
    }

    /** */
    private static class AttrCfg {
        /** */
        final String name;
        /** */
        final Function<Vector, Boolean> obtain;
        /** */
        final List<Class> trueInTypes;

        /** */
        AttrCfg(String name, Function<Vector, Boolean> obtain, Class... trueInTypes) {
            this.name = name;
            this.obtain = obtain;
            this.trueInTypes = Arrays.asList(trueInTypes);
        }
    }

    /** */
    private static class SparseLocalVectorSequentialAccess extends SparseVector {
        /** */
        public SparseLocalVectorSequentialAccess() {
            // No-op.
        }

        /** */
        SparseLocalVectorSequentialAccess(int size) {
            super(size, SEQUENTIAL_ACCESS_MODE);
        }
    }

    /** */
    private static class SparseLocalVectorRandomAccess extends SparseVector {
        /** */
        public SparseLocalVectorRandomAccess() {
            // No-op.
        }

        /** */
        SparseLocalVectorRandomAccess(int size) {
            super(size, RANDOM_ACCESS_MODE);
        }
    }
}
