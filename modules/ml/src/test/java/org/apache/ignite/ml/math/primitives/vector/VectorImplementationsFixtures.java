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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.matrix.impl.DenseMatrix;
import org.apache.ignite.ml.math.primitives.vector.impl.DelegatingVector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.math.primitives.vector.impl.SparseVector;
import org.apache.ignite.ml.math.primitives.vector.impl.VectorizedViewMatrix;
import org.jetbrains.annotations.NotNull;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/** */
class VectorImplementationsFixtures {
    /** */
    private static final List<Supplier<Iterable<Vector>>> suppliers = Arrays.asList(
        (Supplier<Iterable<Vector>>)DenseLocalOnHeapVectorFixture::new,
        (Supplier<Iterable<Vector>>)SparseLocalVectorFixture::new,
        (Supplier<Iterable<Vector>>)DelegatingVectorFixture::new,
        (Supplier<Iterable<Vector>>)MatrixVectorViewFixture::new
    );

    /** */
    void consumeSampleVectors(Consumer<Integer> paramsConsumer, BiConsumer<Vector, String> consumer) {
        for (Supplier<Iterable<Vector>> fixtureSupplier : VectorImplementationsFixtures.suppliers) {
            final Iterable<Vector> fixture = fixtureSupplier.get();

            for (Vector v : fixture) {
                if (paramsConsumer != null)
                    paramsConsumer.accept(v.size());

                consumer.accept(v, fixture.toString());
            }
        }
    }

    /** */
    private static class DenseLocalOnHeapVectorFixture extends VectorSizesExtraFixture<Boolean> {
        /** */
        DenseLocalOnHeapVectorFixture() {
            super("DenseLocalOnHeapVector",
                (size, shallowCp) -> new DenseVector(new double[size], shallowCp),
                "shallow copy", new Boolean[] {false, true, null});
        }
    }

    /** */
    private static class SparseLocalVectorFixture extends VectorSizesExtraFixture<Integer> {
        /** */
        SparseLocalVectorFixture() {
            super("SparseLocalVector", SparseVector::new, "access mode",
                new Integer[] {StorageConstants.SEQUENTIAL_ACCESS_MODE, StorageConstants.RANDOM_ACCESS_MODE, null});
        }
    }

    /** */
    private static class MatrixVectorViewFixture extends VectorSizesExtraFixture<Integer> {
        /** */
        MatrixVectorViewFixture() {
            super("MatrixVectorView",
                MatrixVectorViewFixture::newView,
                "stride kind", new Integer[] {0, 1, 2, null});
        }

        /** */
        private static Vector newView(int size, int strideKind) {
            final Matrix parent = new DenseMatrix(size, size);

            return new VectorizedViewMatrix(parent, 0, 0, strideKind != 1 ? 1 : 0, strideKind != 0 ? 1 : 0);
        }
    }

    /** */
    private static class VectorSizesExtraFixture<T> implements Iterable<Vector> {
        /** */
        private final Supplier<VectorSizesExtraIterator<T>> iter;

        /** */
        private final AtomicReference<String> ctxDescrHolder = new AtomicReference<>("Iterator not started.");

        /** */
        VectorSizesExtraFixture(String vectorKind, BiFunction<Integer, T, Vector> ctor, String extraParamName,
            T[] extras) {
            iter = () -> new VectorSizesExtraIterator<>(vectorKind, ctor, ctxDescrHolder::set, extraParamName, extras);
        }

        /** {@inheritDoc} */
        @NotNull
        @Override public Iterator<Vector> iterator() {
            return iter.get();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            // IMPL NOTE index within bounds is expected to be guaranteed by proper code in this class
            return ctxDescrHolder.get();
        }
    }

    /** */
    private static abstract class VectorSizesFixture implements Iterable<Vector> {
        /** */
        private final Supplier<VectorSizesIterator> iter;

        /** */
        private final AtomicReference<String> ctxDescrHolder = new AtomicReference<>("Iterator not started.");

        /** */
        VectorSizesFixture(String vectorKind, Function<Integer, Vector> ctor) {
            iter = () -> new VectorSizesIterator(vectorKind, ctor, ctxDescrHolder::set);
        }

        /** {@inheritDoc} */
        @NotNull
        @Override public Iterator<Vector> iterator() {
            return iter.get();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            // IMPL NOTE index within bounds is expected to be guaranteed by proper code in this class
            return ctxDescrHolder.get();
        }
    }

    /** */
    private static class VectorSizesExtraIterator<T> extends VectorSizesIterator {
        /** */
        private final T[] extras;
        /** */
        private int extraIdx = 0;
        /** */
        private final BiFunction<Integer, T, Vector> ctor;
        /** */
        private final String extraParamName;

        /**
         * @param vectorKind Descriptive name to use for context logging.
         * @param ctor Constructor for objects to iterate over.
         * @param ctxDescrConsumer Context logging consumer.
         * @param extraParamName Name of extra parameter to iterate over.
         * @param extras Array of extra parameter values to iterate over.
         */
        VectorSizesExtraIterator(String vectorKind, BiFunction<Integer, T, Vector> ctor,
            Consumer<String> ctxDescrConsumer, String extraParamName, T[] extras) {
            super(vectorKind, null, ctxDescrConsumer);

            this.ctor = ctor;
            this.extraParamName = extraParamName;
            this.extras = extras;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return super.hasNext() && hasNextExtra(extraIdx);
        }

        /** {@inheritDoc} */
        @Override void nextIdx() {
            assert extras[extraIdx] != null
                : "Index(es) out of bound at " + VectorSizesExtraIterator.this;

            if (hasNextExtra(extraIdx + 1)) {
                extraIdx++;

                return;
            }

            extraIdx = 0;

            super.nextIdx();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            // IMPL NOTE index within bounds is expected to be guaranteed by proper code in this class
            return "{" + super.toString() +
                ", " + extraParamName + "=" + extras[extraIdx] +
                '}';
        }

        /** {@inheritDoc} */
        @Override BiFunction<Integer, Integer, Vector> ctor() {
            return (size, delta) -> ctor.apply(size + delta, extras[extraIdx]);
        }

        /** */
        void selfTest() {
            final Set<Integer> extraIdxs = new HashSet<>();

            int cnt = 0;

            while (hasNext()) {
                assertNotNull("Expect not null vector at " + this, next());

                if (extras[extraIdx] != null)
                    extraIdxs.add(extraIdx);

                cnt++;
            }

            assertEquals("Extra param tested", extraIdxs.size(), extras.length - 1);

            assertEquals("Combinations tested mismatch.",
                7 * 3 * (extras.length - 1), cnt);
        }

        /** */
        private boolean hasNextExtra(int idx) {
            return extras[idx] != null;
        }
    }

    /** */
    private static class VectorSizesIterator extends TwoParamsIterator<Integer, Integer> {
        /** */
        private final Function<Integer, Vector> ctor;

        /** */
        VectorSizesIterator(String vectorKind, Function<Integer, Vector> ctor, Consumer<String> ctxDescrConsumer) {
            super(vectorKind, null, ctxDescrConsumer,
                "size", new Integer[] {2, 4, 8, 16, 32, 64, 128, null},
                "size delta", new Integer[] {-1, 0, 1, null});

            this.ctor = ctor;
        }

        /** {@inheritDoc} */
        @Override BiFunction<Integer, Integer, Vector> ctor() {
            return (size, delta) -> ctor.apply(size + delta);
        }
    }

    /** */
    private static class TwoParamsIterator<T, U> implements Iterator<Vector> {
        /** */
        private final T params1[];

        /** */
        private final U params2[];

        /** */
        private final String vectorKind;

        /** */
        private final String param1Name;

        /** */
        private final String param2Name;

        /** */
        private final BiFunction<T, U, Vector> ctor;

        /** */
        private final Consumer<String> ctxDescrConsumer;

        /** */
        private int param1Idx = 0;

        /** */
        private int param2Idx = 0;

        /** */
        TwoParamsIterator(String vectorKind, BiFunction<T, U, Vector> ctor,
            Consumer<String> ctxDescrConsumer, String param1Name, T[] params1, String param2Name, U[] params2) {
            this.param1Name = param1Name;
            this.params1 = params1;

            this.param2Name = param2Name;
            this.params2 = params2;

            this.vectorKind = vectorKind;

            this.ctor = ctor;

            this.ctxDescrConsumer = ctxDescrConsumer;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return hasNextParam1(param1Idx) && hasNextParam2(param2Idx);
        }

        /** {@inheritDoc} */
        @Override public Vector next() {
            if (!hasNext())
                throw new NoSuchElementException(TwoParamsIterator.this.toString());

            if (ctxDescrConsumer != null)
                ctxDescrConsumer.accept(toString());

            Vector res = ctor().apply(params1[param1Idx], params2[param2Idx]);

            nextIdx();

            return res;
        }

        /** */
        void selfTest() {
            final Set<Integer> sizeIdxs = new HashSet<>(), deltaIdxs = new HashSet<>();

            int cnt = 0;

            while (hasNext()) {
                assertNotNull("Expect not null vector at " + this, next());

                if (params1[param1Idx] != null)
                    sizeIdxs.add(param1Idx);

                if (params2[param2Idx] != null)
                    deltaIdxs.add(param2Idx);

                cnt++;
            }

            assertEquals("Sizes tested mismatch.", sizeIdxs.size(), params1.length - 1);

            assertEquals("Deltas tested", deltaIdxs.size(), params2.length - 1);

            assertEquals("Combinations tested mismatch.",
                (params1.length - 1) * (params2.length - 1), cnt);
        }

        /** IMPL NOTE override in subclasses if needed */
        void nextIdx() {
            assert params1[param1Idx] != null && params2[param2Idx] != null
                : "Index(es) out of bound at " + TwoParamsIterator.this;

            if (hasNextParam2(param2Idx + 1)) {
                param2Idx++;

                return;
            }

            param2Idx = 0;

            param1Idx++;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            // IMPL NOTE index within bounds is expected to be guaranteed by proper code in this class
            return vectorKind + "{" + param1Name + "=" + params1[param1Idx] +
                ", " + param2Name + "=" + params2[param2Idx] +
                '}';
        }

        /** IMPL NOTE override in subclasses if needed */
        BiFunction<T, U, Vector> ctor() {
            return ctor;
        }

        /** */
        private boolean hasNextParam1(int idx) {
            return params1[idx] != null;
        }

        /** */
        private boolean hasNextParam2(int idx) {
            return params2[idx] != null;
        }
    }

    /** Delegating vector with dense local onheap vector */
    private static class DelegatingVectorFixture implements Iterable<Vector> {

        /** */
        private final Supplier<VectorSizesExtraIterator<Boolean>> iter;

        /** */
        private final AtomicReference<String> ctxDescrHolder = new AtomicReference<>("Iterator not started.");

        /** */
        DelegatingVectorFixture() {
            iter = () -> new VectorSizesExtraIterator<>("DelegatingVector with DenseLocalOnHeapVector",
                (size, shallowCp) -> new DelegatingVector(new DenseVector(new double[size], shallowCp)),
                ctxDescrHolder::set, "shallow copy", new Boolean[] {false, true, null});
        }

        /** {@inheritDoc} */
        @NotNull
        @Override public Iterator<Vector> iterator() {
            return iter.get();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            // IMPL NOTE index within bounds is expected to be guaranteed by proper code in this class
            return ctxDescrHolder.get();
        }
    }
}
