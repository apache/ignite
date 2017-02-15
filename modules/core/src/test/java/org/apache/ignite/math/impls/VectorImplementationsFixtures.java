package org.apache.ignite.math.impls;

import org.apache.ignite.math.Vector;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

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
        }
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
        new DenseLocalOnHeapVectorFixture().selfTest();

        new DenseLocalOffHeapVectorFixture().selfTest();
    }

    /** */
    private static class DenseLocalOnHeapVectorFixture implements Iterable<Vector> {
        /** */ private static final Integer sizes[] = new Integer[] {1, 2, 4, 8, 16, 32, 64, 128, null};

        /** */ private static final Integer deltas[] = new Integer[] {-1, 0, 1, null};

        /** */ private static final Boolean shallowCps[] = new Boolean[] {false, true, null};

        /** */ private int sizeIdx = 0;

        /** */ private int deltaIdx = 0;

        /** */ private int shallowCpIdx = 0;

        /** {@inheritDoc} */
        @Override public Iterator<Vector> iterator() {
            return new Iterator<Vector>() {

                /** {@inheritDoc} */
                @Override public boolean hasNext() {
                    return hasNextSize(sizeIdx) && hasNextDelta(deltaIdx) && hasNextShallowCp(shallowCpIdx);
                }

                /** {@inheritDoc} */
                @Override public Vector next() {
                    if (!hasNext())
                        throw new NoSuchElementException(DenseLocalOnHeapVectorFixture.this.toString());

                    assert sizes[sizeIdx] != null && deltas[deltaIdx] != null && shallowCps[shallowCpIdx] != null
                        : "Index(es) out of bound at " + DenseLocalOnHeapVectorFixture.this;

                    Vector res = new DenseLocalOnHeapVector(new double[sizes[sizeIdx] + deltas[deltaIdx]],
                        shallowCps[shallowCpIdx]);

                    nextIdx();

                    return res;
                }

                private void nextIdx() {
                    if (hasNextShallowCp(shallowCpIdx + 1)) {
                        shallowCpIdx++;

                        return;
                    }

                    if (hasNextDelta(deltaIdx + 1)) {
                        shallowCpIdx = 0;

                        deltaIdx++;

                        return;
                    }

                    shallowCpIdx = 0;

                    deltaIdx = 0;

                    sizeIdx++;
                }
            };
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            // IMPL NOTE index within bounds is expected to be guaranteed by proper code in this class
            return "DenseLocalOnHeapVectorFixture{" + "size=" + sizes[sizeIdx] +
                ", size delta=" + deltas[deltaIdx] +
                ", shallowCopy=" + shallowCps[shallowCpIdx] +
                '}';
        }

        /** */
        void selfTest() {
            final Set<Integer> sizeIdxs = new HashSet<>(), deltaIdxs = new HashSet<>(), shallowCpIdxs = new HashSet<>();

            int cnt = 0;

            for (Vector v : this) {
                assertNotNull("Expect not null vector at " + this, v);

                if (sizes[sizeIdx] != null)
                    sizeIdxs.add(sizeIdx);

                if (deltas[deltaIdx] != null)
                    deltaIdxs.add(deltaIdx);

                if (shallowCps[shallowCpIdx] != null)
                    shallowCpIdxs.add(shallowCpIdx);

                cnt++;
            }

            assertEquals("Sizes tested mismatch.", sizeIdxs.size(), sizes.length - 1);

            assertEquals("Deltas tested", deltaIdxs.size(), deltas.length - 1);

            assertEquals("ShallowCp tested", shallowCpIdxs.size(), shallowCps.length - 1);

            assertEquals("Combinations tested mismatch.",
                (sizes.length - 1) * (deltas.length - 1) * (shallowCps.length - 1), cnt);
        }

        /** */
        private boolean hasNextSize(int idx) {
            return sizes[idx] != null;
        }

        /** */
        private boolean hasNextDelta(int idx) {
            return deltas[idx] != null;
        }

        /** */
        private boolean hasNextShallowCp(int idx) {
            return shallowCps[idx] != null;
        }
    }

    /** */
    private static class DenseLocalOffHeapVectorFixture implements Iterable<Vector> {
        /** */ private static final Integer sizes[] = new Integer[] {1, 2, 4, 8, 16, 32, 64, 128, null};

        /** */ private static final Integer deltas[] = new Integer[] {-1, 0, 1, null};

        /** */ private int sizeIdx = 0;

        /** */ private int deltaIdx = 0;

        /** {@inheritDoc} */
        @Override public Iterator<Vector> iterator() {
            return new Iterator<Vector>() {

                /** {@inheritDoc} */
                @Override public boolean hasNext() {
                    return hasNextSize(sizeIdx) && hasNextDelta(deltaIdx);
                }

                /** {@inheritDoc} */
                @Override public Vector next() {
                    if (!hasNext())
                        throw new NoSuchElementException(DenseLocalOffHeapVectorFixture.this.toString());

                    assert sizes[sizeIdx] != null && deltas[deltaIdx] != null
                        : "Index(es) out of bound at " + DenseLocalOffHeapVectorFixture.this;

                    Vector res = new DenseLocalOffHeapVector(sizes[sizeIdx] + deltas[deltaIdx]);

                    nextIdx();

                    return res;
                }

                private void nextIdx() {
                    if (hasNextDelta(deltaIdx + 1)) {
                        deltaIdx++;

                        return;
                    }

                    deltaIdx = 0;

                    sizeIdx++;
                }
            };
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            // IMPL NOTE index within bounds is expected to be guaranteed by proper code in this class
            return "DenseLocalOffHeapVectorFixture{" + "size=" + sizes[sizeIdx] +
                ", size delta=" + deltas[deltaIdx] +
                '}';
        }

        /** */
        void selfTest() {
            final Set<Integer> sizeIdxs = new HashSet<>(), deltaIdxs = new HashSet<>();

            int cnt = 0;

            for (Vector v : this) {
                assertNotNull("Expect not null vector at " + this, v);

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
