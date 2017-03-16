package org.apache.ignite.math.decompositions;

import org.apache.ignite.math.Matrix;
import org.apache.ignite.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link LUDecomposition}.
 *
 * TODO: WIP
 */
public class LUDecompositionTest {
    private Matrix testMatrix;
    private Matrix testL;
    private Matrix testU;

    private LUDecomposition luDecomposition;

    /** */
    @Before
    public void setUp(){
        double[][] rawMatrix = {
            {2.0d, 1.0d, 1.0d, 0.0d},
            {4.0d, 3.0d, 3.0d, 1.0d},
            {8.0d, 7.0d, 9.0d, 5.0d},
            {6.0d, 7.0d, 9.0d, 8.0d}};
        double[][] rawL = {
            {1.0d, 0.0d, 0.0d, 0.0d},
            {3.0d/4.0d, 1.0d, 0.0d, 0.0d},
            {1.0d/2.0d, -2.0d/7.0d, 1.0d, 0.0d},
            {1.0d/4.0d, -3.0d/7.0d, 1.0d/3.0d, 1.0d}};
        double[][] rawU = {
            {8.0d, 7.0d, 9.0d, 5.0d},
            {0.0d, 7.0d/4.0d, 9.0d/4.0d, 17.0d/4.0d},
            {0.0d, 0.0d, -6.0d/4.0d, -2.0d/7.0d},
            {0.0d, 0.0d, 0.0d, 2.0d/3.0d}};

        testMatrix = new DenseLocalOnHeapMatrix(rawMatrix);
        testL = new DenseLocalOnHeapMatrix(rawL);
        testU = new DenseLocalOnHeapMatrix(rawU);

        luDecomposition = new LUDecomposition(testMatrix);
    }

    /** */
    @Test
    public void getL() throws Exception {
        Matrix luDecompositionL = luDecomposition.getL();

        assertTrue(luDecompositionL.equals(testL));
    }

    /** */
    @Test
    public void getU() throws Exception {
        Matrix luDecompositionU = luDecomposition.getU();

        assertTrue(luDecompositionU.equals(testU));
    }

    /** */
    @Test
    public void getP() throws Exception {

    }

    /** */
    @Test
    public void getPivot() throws Exception {

    }

}