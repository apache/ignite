package org.apache.ignite.math.impls;

import org.apache.ignite.math.Matrix;
import org.apache.ignite.math.ExternalizeTest;

import java.util.function.BiConsumer;

/**
 * Tests for {@link Matrix} implementations.
 */
public class MatrixImplementationsTest extends ExternalizeTest<Matrix> {
    /** */
    private void consumeSampleVectors(BiConsumer<Integer, Integer> paramsConsumer, BiConsumer<Matrix, String> consumer) {
        new MatrixImplementationFixtures().consumeSampleMatrix(paramsConsumer, consumer);
    }

    @Override
    public void externalizeTest() {
        consumeSampleVectors(null, (m, desc) -> {
            externalizeTest(m);
        });
    }
}
