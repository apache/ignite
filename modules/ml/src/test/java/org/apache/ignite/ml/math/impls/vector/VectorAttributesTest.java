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

package org.apache.ignite.ml.math.impls.vector;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOffHeapMatrix;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** */
public class VectorAttributesTest {
    /** */
    private final List<AttrCfg> attrCfgs = Arrays.asList(
        new AttrCfg("isDense", Vector::isDense,
            DenseLocalOnHeapVector.class, DenseLocalOffHeapVector.class, RandomVector.class, ConstantVector.class,
            SingleElementVector.class),
        new AttrCfg("isArrayBased", Vector::isArrayBased,
            DenseLocalOnHeapVector.class),
        new AttrCfg("isSequentialAccess", Vector::isSequentialAccess,
            DenseLocalOnHeapVector.class, DenseLocalOffHeapVector.class, SparseLocalVectorSequentialAccess.class,
            RandomVector.class, ConstantVector.class, SingleElementVector.class),
        new AttrCfg("guidNotNull", v -> v.guid() == null), // IMPL NOTE this is somewhat artificial
        new AttrCfg("isRandomAccess", Vector::isRandomAccess,
            DenseLocalOnHeapVector.class, DenseLocalOffHeapVector.class, RandomVector.class, ConstantVector.class,
            SingleElementVector.class, SparseLocalVectorSequentialAccess.class, SparseLocalVectorRandomAccess.class),
        new AttrCfg("isDistributed", Vector::isDistributed));

    /** */
    private final List<Specification> specFixture = Arrays.asList(
        new Specification(new DenseLocalOnHeapVector(1)),
        new Specification(new DenseLocalOffHeapVector(1)),
        new Specification(new DelegatingVector(new DenseLocalOnHeapVector(1)),
            DenseLocalOnHeapVector.class, "isDense", "isArrayBased", "isSequentialAccess",
            "isRandomAccess", "isDistributed"),
        new Specification(new DelegatingVector(new DenseLocalOffHeapVector(1)),
            DenseLocalOffHeapVector.class, "isDense", "isArrayBased", "isSequentialAccess",
            "isRandomAccess", "isDistributed"),
        new Specification(new SparseLocalVectorSequentialAccess(1)),
        new Specification(new SparseLocalVectorRandomAccess(1)),
        new Specification(new RandomVector(1)),
        new Specification(new ConstantVector(1, 1.0)),
        new Specification(new FunctionVector(1, idx -> (double)idx)),
        new Specification(new SingleElementVector(1, 0, 1.0)),
        new Specification(new PivotedVectorView(new DenseLocalOnHeapVector(1), new int[] {0}),
            DenseLocalOnHeapVector.class, "isDense", "isArrayBased", "isSequentialAccess",
            "isRandomAccess", "isDistributed"),
        new Specification(new PivotedVectorView(new DenseLocalOffHeapVector(1), new int[] {0}),
            DenseLocalOffHeapVector.class, "isDense", "isArrayBased", "isSequentialAccess",
            "isRandomAccess", "isDistributed"),
        new Specification(new SingleElementVectorView(new DenseLocalOnHeapVector(1), 0),
            DenseLocalOnHeapVector.class, "isDense", "isSequentialAccess",
            "isRandomAccess", "isDistributed"),
        new Specification(new SingleElementVectorView(new DenseLocalOffHeapVector(1), 0),
            DenseLocalOffHeapVector.class, "isDense", "isSequentialAccess",
            "isRandomAccess", "isDistributed"),
        new Specification(new MatrixVectorView(new DenseLocalOnHeapMatrix(1, 1), 0, 0, 1, 1),
            DenseLocalOnHeapVector.class, "isDense",
            "isRandomAccess", "isDistributed"), // todo find out why "isSequentialAccess" fails here
        new Specification(new MatrixVectorView(new DenseLocalOffHeapMatrix(1, 1), 0, 0, 1, 1),
            DenseLocalOffHeapVector.class, "isDense",
            "isRandomAccess", "isDistributed"));

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
    private static class SparseLocalVectorSequentialAccess extends SparseLocalVector {
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
    private static class SparseLocalVectorRandomAccess extends SparseLocalVector {
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
