package org.apache.ignite.math.impls;

import org.apache.ignite.math.Matrix;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/** */
public class MatrixImplementationFixtures {
    private static final List<Supplier<Iterable<Matrix>>> suppliers = Arrays.asList(
            new Supplier<Iterable<Matrix>>() {
                @Override public Iterable<Matrix> get() {
                    return new MatrixImplementationFixtures.SparseLocalMatrixFixture();
                }
            },
            new Supplier<Iterable<Matrix>>() {
                @Override public Iterable<Matrix> get() {
                    return new MatrixImplementationFixtures.SparseLocalRowMatrixFixture();
                }
            }
    );

    void consumeSampleMatrix(BiConsumer<Integer, Integer> paramsConsumer, BiConsumer<Matrix, String> consumer){
        for (Supplier<Iterable<Matrix>> fixtureSupplier : suppliers) {
            final Iterable<Matrix> fixture = fixtureSupplier.get();

            for (Matrix matrixStorage : fixture) {
                if (paramsConsumer != null)
                    paramsConsumer.accept(matrixStorage.rowSize(), matrixStorage.columnSize());

                consumer.accept(matrixStorage, fixture.toString());
            }
        }
    }

    private static class SparseLocalMatrixFixture implements Iterable<Matrix>{
        private final Integer[] rows = new Integer[] {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 512, 1024, null};
        private final Integer[] cols = new Integer[] {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 1024, 512, null};
        private int sizeIdx = 0;

        @Override public Iterator<Matrix> iterator() {
            return new Iterator<Matrix>() {
                @Override public boolean hasNext() {
                    return hasNextCol(sizeIdx) && hasNextRow(sizeIdx);
                }

                @Override public Matrix next() {
                    if (!hasNext())
                        throw new NoSuchElementException(MatrixImplementationFixtures.SparseLocalMatrixFixture.this.toString());

                    Matrix storage = new SparseLocalOnHeapMatrix(rows[sizeIdx], cols[sizeIdx]);

                    nextIdx();

                    return storage;
                }

                private void nextIdx(){
                    sizeIdx++;
                }
            };
        }

        @Override public String toString() {
            return "SparseLocalMatrixFixture{ " + "rows=" + rows[sizeIdx] + " ,cols=" + cols[sizeIdx] + "}";
        }

        private boolean hasNextRow(int idx){
            return rows[idx] != null;
        }

        private boolean hasNextCol(int idx){
            return cols[idx] != null;
        }
    }

    private static class SparseLocalRowMatrixFixture implements Iterable<Matrix>{
        private final Integer[] rows = new Integer[] {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 512, 1024, null};
        private final Integer[] cols = new Integer[] {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 1024, 512, null};
        private final Boolean[] randomAccess = new Boolean[] {true, false, null};
        private int sizeIdx = 0;
        private int modeIdx = 0;

        @Override public Iterator<Matrix> iterator() {
            return new Iterator<Matrix>() {
                @Override public boolean hasNext() {
                    return hasNextCol(sizeIdx) && hasNextRow(sizeIdx) && hasNextMode(modeIdx);
                }

                @Override public Matrix next() {
                    if (!hasNext())
                        throw new NoSuchElementException(MatrixImplementationFixtures.SparseLocalRowMatrixFixture.this.toString());

                    Matrix storage = new SparseLocalOnHeapRowMatrix(rows[sizeIdx], cols[sizeIdx], randomAccess[modeIdx]);

                    nextIdx();

                    return storage;
                }

                private void nextIdx(){
                    if (hasNextMode(modeIdx + 1)){
                        modeIdx++;

                        return;
                    }

                    modeIdx = 0;
                    sizeIdx++;
                }
            };
        }

        @Override public String toString() {
            return "SparseLocalRowMatrixFixture{ " + "rows=" + rows[sizeIdx] + " ,cols=" + cols[sizeIdx] +
                    " ,mode=" + randomAccess[modeIdx] + "}";
        }

        private boolean hasNextRow(int idx){
            return rows[idx] != null;
        }

        private boolean hasNextCol(int idx){
            return cols[idx] != null;
        }

        private boolean hasNextMode(int idx){
            return randomAccess[idx] != null;
        }
    }

}
