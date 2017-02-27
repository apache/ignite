package org.apache.ignite.math.impls.storage;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import org.apache.ignite.math.MatrixStorage;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link MatrixStorage} implementations.
 */
public class MatrixStorageImplementationTest {
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

    /** */
    private void consumeSampleVectors(BiConsumer<Integer, Integer> paramsConsumer, BiConsumer<MatrixStorage, String> consumer) {
        new MatrixStorageFixtures().consumeSampleStorages(paramsConsumer, consumer);
    }
}
