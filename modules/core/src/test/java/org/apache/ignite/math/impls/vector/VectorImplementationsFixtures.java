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

package org.apache.ignite.math.impls.vector;

import org.apache.ignite.math.Vector;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/** */
class VectorImplementationsFixtures {
    /** */
    private static final List<Supplier<Iterable<Vector>>> suppliers = Arrays.asList(
        new Supplier<Iterable<Vector>>() {
            /** {@inheritDoc} */
            @Override public Iterable<Vector> get() {
                return new DenseLocalOnHeapVectorFixture();
            }
        },
        new Supplier<Iterable<Vector>>() {
            /** {@inheritDoc} */
            @Override public Iterable<Vector> get() {
                return new DenseLocalOffHeapVectorFixture();
            }
        },
        new Supplier<Iterable<Vector>>() {
            /** {@inheritDoc} */
            @Override public Iterable<Vector> get() {
                return new SparseLocalOnHeapVectorFixture();
            }
        },
        new Supplier<Iterable<Vector>>() {
        /** {@inheritDoc} */
        @Override public Iterable<Vector> get() {
            return new SparseLocalOffHeapVectorFixture();
        }
    } // todo add Supplier for RandomVectorFixture after testing and bugfixes are completed
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
    void selfTest() {
        new VectorSizesExtraIterator<>("VectorSizesExtraIterator test",
            (size, shallowCp) -> new DenseLocalOnHeapVector(new double[size], shallowCp),
            null, new Boolean[] {false, true, null}).selfTest();

        new VectorSizesIterator("VectorSizesIterator test", DenseLocalOffHeapVector::new, null).selfTest();
    }

    /** */
    private static class DenseLocalOnHeapVectorFixture implements Iterable<Vector> {
        /** */ private final Supplier<VectorSizesExtraIterator<Boolean>> iter;

        /** */ private final AtomicReference<String> ctxDescrHolder = new AtomicReference<>("Iterator not started.");

        /** */
        DenseLocalOnHeapVectorFixture() {
            iter = () -> new VectorSizesExtraIterator<>("DenseLocalOnHeapVector",
                (size, shallowCp) -> new DenseLocalOnHeapVector(new double[size], shallowCp),
                ctxDescrHolder::set, new Boolean[] {false, true, null});
        }

        /** {@inheritDoc} */
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
    private static class DenseLocalOffHeapVectorFixture extends VectorSizesFixture {
        /** */
        DenseLocalOffHeapVectorFixture() {
            super("DenseLocalOffHeapVector", DenseLocalOffHeapVector::new);
        }
    }

    /** */
    private static class SparseLocalOnHeapVectorFixture implements Iterable<Vector> {
        /** */ private final Supplier<VectorSizesExtraIterator<Integer>> iter;

        /** */ private final AtomicReference<String> ctxDescrHolder = new AtomicReference<>("Iterator not started.");

        /** */
        SparseLocalOnHeapVectorFixture() {
            iter = () -> new VectorSizesExtraIterator<>("SparseLocalOnHeapVector", SparseLocalOnHeapVector::new,
                ctxDescrHolder::set, new Integer[] {0, 1, null});
        }

        /** {@inheritDoc} */
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
    private static class SparseLocalOffHeapVectorFixture extends VectorSizesFixture{
        SparseLocalOffHeapVectorFixture() {
            super("SparseLocalOffHeapVector", SparseLocalOffHeapVector::new);
        }
    }

    /** */
    private static class RandomVectorFixture extends VectorSizesFixture {
        /** */
        RandomVectorFixture() {
            super("RandomVector", RandomVector::new);
        }
    }

    /** */
    private static abstract class VectorSizesFixture implements Iterable<Vector> {
        /** */ private final Supplier<VectorSizesIterator> iter;

        /** */ private final AtomicReference<String> ctxDescrHolder = new AtomicReference<>("Iterator not started.");

        /** */
        VectorSizesFixture(String vectorKind, Function<Integer, Vector> ctor) {
            iter = () -> new VectorSizesIterator(vectorKind, ctor, ctxDescrHolder::set);
        }

        /** {@inheritDoc} */
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
        /** */ private final T[] extras;
        /** */ private int extraIdx = 0;
        /** */ private final BiFunction<Integer, T, Vector> ctor;

        /**
         *
         * @param vectorKind Descriptive name to use for context logging.
         * @param ctor Constructor for objects to iterate over.
         * @param ctxDescrConsumer Context logging consumer.
         * @param extras Array of extra parameter values to iterate over.
         */
        VectorSizesExtraIterator(String vectorKind, BiFunction<Integer, T, Vector> ctor,
            Consumer<String> ctxDescrConsumer, T[] extras) {
            super(vectorKind, null, ctxDescrConsumer);

            this.ctor = ctor;
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
                ", extra param=" + extras[extraIdx] +
                '}';
        }

        /** {@inheritDoc} */
        @Override Function<Integer, Vector> ctor() {
            return (size) -> ctor.apply(size, extras[extraIdx]);
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
                8 * 3 * (extras.length - 1), cnt);
        }

        /** */
        private boolean hasNextExtra(int idx) {
            return extras[idx] != null;
        }
    }

    /** */
    private static class VectorSizesIterator implements Iterator<Vector> {
        /** */ private static final Integer sizes[] = new Integer[] {1, 2, 4, 8, 16, 32, 64, 128, null};

        /** */ private static final Integer deltas[] = new Integer[] {-1, 0, 1, null};

        /** */ private final String vectorKind;

        /** */ private final Function<Integer, Vector> ctor;

        /** */ private final Consumer<String> ctxDescrConsumer;

        /** */ private int sizeIdx = 0;

        /** */ private int deltaIdx = 0;

        /** */
        VectorSizesIterator(String vectorKind, Function<Integer, Vector> ctor, Consumer<String> ctxDescrConsumer) {
            this.vectorKind = vectorKind;

            this.ctor = ctor;

            this.ctxDescrConsumer = ctxDescrConsumer;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return hasNextSize(sizeIdx) && hasNextDelta(deltaIdx);
        }

        /** {@inheritDoc} */
        @Override public Vector next() {
            if (!hasNext())
                throw new NoSuchElementException(VectorSizesIterator.this.toString());

            if (ctxDescrConsumer != null)
                ctxDescrConsumer.accept(toString());

            Vector res = ctor().apply(sizes[sizeIdx] + deltas[deltaIdx]);

            nextIdx();

            return res;
        }

        /** IMPL NOTE override in subclasses if needed */
        void nextIdx() {
            assert sizes[sizeIdx] != null && deltas[deltaIdx] != null
                : "Index(es) out of bound at " + VectorSizesIterator.this;

            if (hasNextDelta(deltaIdx + 1)) {
                deltaIdx++;

                return;
            }

            deltaIdx = 0;

            sizeIdx++;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            // IMPL NOTE index within bounds is expected to be guaranteed by proper code in this class
            return vectorKind + "{" + "size=" + sizes[sizeIdx] +
                ", size delta=" + deltas[deltaIdx] +
                '}';
        }

        /** IMPL NOTE override in subclasses if needed */
        Function<Integer, Vector> ctor() { return ctor; }

        /** */
        void selfTest() {
            final Set<Integer> sizeIdxs = new HashSet<>(), deltaIdxs = new HashSet<>();

            int cnt = 0;

            while (hasNext()) {
                assertNotNull("Expect not null vector at " + this, next());

                if (sizes[sizeIdx] != null)
                    sizeIdxs.add(sizeIdx);

                if (deltas[deltaIdx] != null)
                    deltaIdxs.add(deltaIdx);

                cnt++;
            }

            assertEquals("Sizes tested mismatch.", sizeIdxs.size(), sizes.length - 1);

            assertEquals("Deltas tested", deltaIdxs.size(), deltas.length - 1);

            assertEquals("Combinations tested mismatch.",
                (sizes.length - 1) * (deltas.length - 1), cnt);
        }

        /** */
        private boolean hasNextSize(int idx) {
            return sizes[idx] != null;
        }

        /** */
        private boolean hasNextDelta(int idx) {
            return deltas[idx] != null;
        }
    }
}
