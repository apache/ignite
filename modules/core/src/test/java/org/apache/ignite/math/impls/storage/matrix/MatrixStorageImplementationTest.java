package org.apache.ignite.math.impls.storage.matrix;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import org.apache.ignite.math.MatrixStorage;
import org.apache.ignite.math.ExternalizeTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link MatrixStorage} implementations.
 *
 * TODO: add attribute tests.
 */
public class MatrixStorageImplementationTest extends ExternalizeTest<MatrixStorage> {
    /**
     * The columnSize() and the rowSize() test.
     */
    @Test
    public void sizeTest(){
        final AtomicReference<Integer> expRowSize = new AtomicReference<>(0);
        final AtomicReference<Integer> expColSize = new AtomicReference<>(0);

        consumeSampleVectors((x,y) -> {expRowSize.set(x); expColSize.set(y);},
            (ms, desc) -> assertTrue("Expected size for " + desc, expColSize.get().equals(ms.columnSize()) && expRowSize.get().equals(ms.rowSize())));
    }

    @Test
    public void getSetTest(){
        consumeSampleVectors(null, (ms,desc) -> {
            for (int i = 0; i < ms.rowSize(); i++) {
                for (int j = 0; j < ms.columnSize(); j++) {
                    double random = Math.random();
                    ms.set(i, j, random);
                    assertTrue("Unexpected value for " + desc + " x:" + i + ", y:" + j, Double.compare(random, ms.get(i, j)) == 0);
                }
            }
        });
    }

    @Test
    public void serializationTest(){
        consumeSampleVectors(null, (ms, desc) -> {

        });
    }

    /** */
    private void consumeSampleVectors(BiConsumer<Integer, Integer> paramsConsumer, BiConsumer<MatrixStorage, String> consumer) {
        new MatrixStorageFixtures().consumeSampleStorages(paramsConsumer, consumer);
    }

    @Override
    public void externalizeTest() {
        consumeSampleVectors(null, (ms, desc) -> {
            externalizeTest(ms);
        });
    }
}
