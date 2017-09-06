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

package org.apache.ignite.ml.math.impls.matrix;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.impls.storage.matrix.FunctionMatrixStorage;
import org.jetbrains.annotations.NotNull;

/** */
class MatrixImplementationFixtures {
    /** */
    private static final List<Supplier<Iterable<Matrix>>> suppliers = Arrays.asList(
        (Supplier<Iterable<Matrix>>)DenseLocalOnHeapMatrixFixture::new,
        (Supplier<Iterable<Matrix>>)DenseLocalOffHeapMatrixFixture::new,
        (Supplier<Iterable<Matrix>>)RandomMatrixFixture::new,
        (Supplier<Iterable<Matrix>>)SparseLocalOnHeapMatrixFixture::new,
        (Supplier<Iterable<Matrix>>)PivotedMatrixViewFixture::new,
        (Supplier<Iterable<Matrix>>)MatrixViewFixture::new,
        (Supplier<Iterable<Matrix>>)FunctionMatrixFixture::new,
        (Supplier<Iterable<Matrix>>)DiagonalMatrixFixture::new,
        (Supplier<Iterable<Matrix>>)TransposedMatrixViewFixture::new
    );

    /** */
    void consumeSampleMatrix(BiConsumer<Matrix, String> consumer) {
        for (Supplier<Iterable<Matrix>> fixtureSupplier : suppliers) {
            final Iterable<Matrix> fixture = fixtureSupplier.get();

            for (Matrix matrix : fixture) {
                consumer.accept(matrix, fixture.toString());

                matrix.destroy();
            }
        }
    }

    /** */
    private static class DenseLocalOnHeapMatrixFixture extends MatrixSizeIterator {
        /** */
        DenseLocalOnHeapMatrixFixture() {
            super(DenseLocalOnHeapMatrix::new, "DenseLocalOnHeapMatrix");
        }
    }

    /** */
    private static class DenseLocalOffHeapMatrixFixture extends MatrixSizeIterator {
        /** */
        DenseLocalOffHeapMatrixFixture() {
            super(DenseLocalOffHeapMatrix::new, "DenseLocalOffHeapMatrix");
        }
    }

    /** */
    private static class RandomMatrixFixture extends MatrixSizeIterator {
        /** */
        RandomMatrixFixture() {
            super(RandomMatrix::new, "RandomMatrix");
        }
    }

    /** */
    private static class SparseLocalOnHeapMatrixFixture extends MatrixSizeIterator {
        /** */
        SparseLocalOnHeapMatrixFixture() {
            super(SparseLocalOnHeapMatrix::new, "SparseLocalOnHeapMatrix");
        }
    }

    /** */
    private static class PivotedMatrixViewFixture extends WrapperMatrixIterator {
        /** */
        PivotedMatrixViewFixture() {
            super(PivotedMatrixView::new, "PivotedMatrixView over DenseLocalOnHeapMatrix");
        }
    }

    /** */
    private static class MatrixViewFixture extends WrapperMatrixIterator {
        /** */
        MatrixViewFixture() {
            super((matrix) -> new MatrixView(matrix, 0, 0, matrix.rowSize(), matrix.columnSize()),
                "MatrixView over DenseLocalOnHeapMatrix");
        }
    }

    /** */
    private static class FunctionMatrixFixture extends WrapperMatrixIterator {
        /** */
        FunctionMatrixFixture() {
            super(FunctionMatrixForTest::new, "FunctionMatrix wrapping DenseLocalOnHeapMatrix");
        }
    }

    /** */
    private static class DiagonalMatrixFixture extends DiagonalIterator {
        /** */
        DiagonalMatrixFixture() {
            super(DenseLocalOnHeapMatrix::new, "DiagonalMatrix over DenseLocalOnHeapMatrix");
        }

        /** {@inheritDoc} */
        @NotNull
        @Override public Iterator<Matrix> iterator() {
            return new Iterator<Matrix>() {
                /** {@inheritDoc} */
                @Override public boolean hasNext() {
                    return hasNextSize(getSizeIdx());
                }

                /** {@inheritDoc} */
                @Override public Matrix next() {
                    assert getSize(getSizeIdx()) == 1 : "Only size 1 allowed for diagonal matrix fixture.";

                    Matrix matrix = getConstructor().apply(getSize(getSizeIdx()), getSize(getSizeIdx()));

                    nextIdx();

                    return new DiagonalMatrix(matrix);
                }
            };
        }
    }

    /** */
    private static class TransposedMatrixViewFixture extends WrapperMatrixIterator {
        /** */
        TransposedMatrixViewFixture() {
            super(TransposedMatrixView::new, "TransposedMatrixView over DenseLocalOnHeapMatrix");
        }
    }

    /** */
    private static abstract class DiagonalIterator implements Iterable<Matrix> {
        /** */
        private final Integer[] sizes = new Integer[] {1, null};
        /** */
        private int sizeIdx = 0;

        /** */
        private BiFunction<Integer, Integer, ? extends Matrix> constructor;
        /** */
        private String desc;

        /** */
        DiagonalIterator(BiFunction<Integer, Integer, ? extends Matrix> constructor, String desc) {
            this.constructor = constructor;
            this.desc = desc;
        }

        /** */
        public BiFunction<Integer, Integer, ? extends Matrix> getConstructor() {
            return constructor;
        }

        /** */
        int getSizeIdx() {
            return sizeIdx;
        }

        /** */
        @Override public String toString() {
            return desc + "{rows=" + sizes[sizeIdx] + ", cols=" + sizes[sizeIdx] + "}";
        }

        /** */
        boolean hasNextSize(int idx) {
            return sizes[idx] != null;
        }

        /** */
        Integer getSize(int idx) {
            return sizes[idx];
        }

        /** */
        void nextIdx() {
            sizeIdx++;
        }
    }

    /** */
    private static class WrapperMatrixIterator extends MatrixSizeIterator {
        /** */
        private final Function<Matrix, Matrix> wrapperCtor;

        /** */
        WrapperMatrixIterator(Function<Matrix, Matrix> wrapperCtor, String desc) {
            super(DenseLocalOnHeapMatrix::new, desc);

            this.wrapperCtor = wrapperCtor;
        }

        /** {@inheritDoc} */
        @NotNull
        @Override public Iterator<Matrix> iterator() {
            return new Iterator<Matrix>() {
                /** {@inheritDoc} */
                @Override public boolean hasNext() {
                    return hasNextCol(getSizeIdx()) && hasNextRow(getSizeIdx());
                }

                /** {@inheritDoc} */
                @Override public Matrix next() {
                    Matrix matrix = getConstructor().apply(getRow(getSizeIdx()), getCol(getSizeIdx()));

                    nextIdx();

                    return wrapperCtor.apply(matrix);
                }
            };
        }
    }

    /** */
    private static class MatrixSizeIterator implements Iterable<Matrix> {
        /** */
        private final Integer[] rows = new Integer[] {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 512, 1024, null};
        /** */
        private final Integer[] cols = new Integer[] {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 1024, 512, null};
        /** */
        private int sizeIdx = 0;

        /** */
        private BiFunction<Integer, Integer, ? extends Matrix> constructor;
        /** */
        private String desc;

        /** */
        MatrixSizeIterator(BiFunction<Integer, Integer, ? extends Matrix> constructor, String desc) {
            this.constructor = constructor;
            this.desc = desc;
        }

        /** */
        public BiFunction<Integer, Integer, ? extends Matrix> getConstructor() {
            return constructor;
        }

        /** */
        int getSizeIdx() {
            return sizeIdx;
        }

        /** */
        @Override public String toString() {
            return desc + "{rows=" + rows[sizeIdx] + ", cols=" + cols[sizeIdx] + "}";
        }

        /** */
        boolean hasNextRow(int idx) {
            return rows[idx] != null;
        }

        /** */
        boolean hasNextCol(int idx) {
            return cols[idx] != null;
        }

        /** */
        Integer getRow(int idx) {
            return rows[idx];
        }

        /** */
        int getCol(int idx) {
            return cols[idx];
        }

        /** {@inheritDoc} */
        @NotNull
        @Override public Iterator<Matrix> iterator() {
            return new Iterator<Matrix>() {
                /** {@inheritDoc} */
                @Override public boolean hasNext() {
                    return hasNextCol(sizeIdx) && hasNextRow(sizeIdx);
                }

                /** {@inheritDoc} */
                @Override public Matrix next() {
                    Matrix matrix = constructor.apply(rows[sizeIdx], cols[sizeIdx]);

                    nextIdx();

                    return matrix;
                }
            };
        }

        /** */
        void nextIdx() {
            sizeIdx++;
        }
    }

    /** Subclass tweaked for serialization */
    private static class FunctionMatrixForTest extends FunctionMatrix {
        /** */
        Matrix underlying;

        /** */
        public FunctionMatrixForTest() {
            // No-op.
        }

        /** */
        FunctionMatrixForTest(Matrix underlying) {
            super(underlying.rowSize(), underlying.columnSize(), underlying::get, underlying::set);

            this.underlying = underlying;
        }

        /** {@inheritDoc} */
        @Override public Matrix copy() {
            return new FunctionMatrixForTest(underlying);
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            super.writeExternal(out);

            out.writeObject(underlying);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            super.readExternal(in);

            underlying = (Matrix)in.readObject();

            setStorage(new FunctionMatrixStorage(underlying.rowSize(), underlying.columnSize(),
                underlying::get, underlying::set));
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = 1;

            res = res * 37 + underlying.hashCode();

            return res;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            FunctionMatrixForTest that = (FunctionMatrixForTest)o;

            return underlying != null ? underlying.equals(that.underlying) : that.underlying == null;
        }
    }
}
