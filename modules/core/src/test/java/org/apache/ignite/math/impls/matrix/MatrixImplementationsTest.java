package org.apache.ignite.math.impls.matrix;

import org.apache.ignite.math.Matrix;
import org.apache.ignite.math.ExternalizeTest;
import org.junit.Test;

import java.util.function.BiConsumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link Matrix} implementations.
 */
public class MatrixImplementationsTest extends ExternalizeTest<Matrix> {
    /** */
    private void consumeSampleVectors(BiConsumer<Integer, Integer> paramsConsumer, BiConsumer<Matrix, String> consumer) {
        new MatrixImplementationFixtures().consumeSampleMatrix(paramsConsumer, consumer);
    }

    /** */
    @Override
    public void externalizeTest() {
        consumeSampleVectors(null, (m, desc) -> {
            externalizeTest(m);
        });
    }

    /** */
    @Test
    public void likeTest(){
        consumeSampleVectors(null, (m, desc) -> {
            Matrix like = m.like(m.rowSize(), m.columnSize());

            assertEquals("Wrong \"like\" matrix for "+ desc + "; Unexpected class: " + like.getClass().toString(),
                    like.getClass(),
                    m.getClass());
            assertEquals("Wrong \"like\" matrix for "+ desc + "; Unexpected rows.", like.rowSize(), m.rowSize());
            assertEquals("Wrong \"like\" matrix for "+ desc + "; Unexpected columns.", like.columnSize(), m.columnSize());
            assertEquals("Wrong \"like\" matrix for "+ desc + "; Unexpected storage class: " + like.getStorage().getClass().toString(),
                    like.getStorage().getClass(),
                    m.getStorage().getClass());
        });
    }

    /** */
    @Test
    public void copyTest(){
        consumeSampleVectors(null, (m, desc) -> {
            Matrix copy = m.copy();
            assertTrue("Incorrect copy for empty matrix " + desc, copy.equals(m));

            fillMatrix(m);
            copy = m.copy();
            assertTrue("Incorrect copy for matrix " + desc, copy.equals(m));
        });
    }

    /** */
    private void fillMatrix(Matrix m){
        for (int i = 0; i < m.rowSize(); i++) {
            for (int j = 0; j < m.columnSize(); j++) {
                m.set(i, j, Math.random());
            }
        }
    }
}
