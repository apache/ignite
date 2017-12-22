package org.apache.ignite.ml.regressions.linear;

import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.Trainer;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.junit.Test;

/**
 * Tests for {@link OLSGradientDescentLinearRegressionTrainer}.
 */
public class OLSGradientDescentLinearRegressionTrainerTest {

    /** */
    private static final double PRECISION = 1e-12;

    /** */
    @Test
    public void testTrainWithoutBias() {
        Trainer<LinearRegressionModel, Matrix> tr = new OLSGradientDescentLinearRegressionTrainer(0, 1e-2, 1000000);
        Matrix data = new DenseLocalOnHeapMatrix(new double[][] {
            {2.0, 1.0},
            {4.0, 2.0}
        });
        LinearRegressionModel model = tr.train(data);
        TestUtils.assertEquals(4, model.predict(new DenseLocalOnHeapVector(new double[] {2})), PRECISION);
        TestUtils.assertEquals(6, model.predict(new DenseLocalOnHeapVector(new double[] {3})), PRECISION);
        TestUtils.assertEquals(8, model.predict(new DenseLocalOnHeapVector(new double[] {4})), PRECISION);
    }

    /** */
    @Test
    public void testTrainWithBias() {
        Trainer<LinearRegressionModel, Matrix> tr = new OLSGradientDescentLinearRegressionTrainer(0, 1e-2, 1000000);
        Matrix data = new DenseLocalOnHeapMatrix(new double[][] {
            {1.0, 0.0},
            {0.0, 1.0}
        });
        LinearRegressionModel model = tr.train(data);
        TestUtils.assertEquals(0.5, model.predict(new DenseLocalOnHeapVector(new double[] {0.5})), PRECISION);
        TestUtils.assertEquals(2, model.predict(new DenseLocalOnHeapVector(new double[] {-1})), PRECISION);
        TestUtils.assertEquals(-1, model.predict(new DenseLocalOnHeapVector(new double[] {2})), PRECISION);
    }

    /** */
    @Test
    public void testTrainOnArtificiallyGeneratedDataSet() {
        Matrix data = new DenseLocalOnHeapMatrix(new double[][] {
            {0.5394105716145406, 0.7664530105584364},
            {0.46072584240549597, 0.18882014088308152},
            {0.9614812268385736, 0.6272831890999323},
            {0.8013739396352404, 0.7511866979735841},
            {0.0648414763301316, 0.15766733678114375},
            {0.4160160984052179, 0.7241249214623566},
            {0.17616905766850877, 0.6539779134426781},
            {0.8572743682997773, 0.4258853387057442},
            {0.24341008185682966, 0.770484186998581},
            {0.5974520929270991, 0.6030016942207411},
            {0.9481585044488966, 0.32879882522074466},
            {0.12467652570994547, 0.35905589788096115},
            {0.5222334605969042, 0.9756185188451999},
            {0.8529348905564488, 0.7708028339176523},
            {0.03594108494406911, 0.8661577433794824},
            {0.16671282262225018, 0.5433700041951284},
            {0.2585151275646834, 0.06336931034877713},
            {0.3320776419010457, 0.5220675193364925},
            {0.9304886547455815, 0.011991303251888241},
            {0.323430787518975, 4.1414167040876304E-4}
        });
        Trainer<LinearRegressionModel, Matrix> tr = new OLSGradientDescentLinearRegressionTrainer(0, 1e-2, 1000000);
        LinearRegressionModel model = tr.train(data);
        TestUtils.assertEquals(0.4712, model.predict(new DenseLocalOnHeapVector(new double[] {0.0})), 1e-4);
        TestUtils.assertEquals(0.4712 + 0.01866, model.predict(new DenseLocalOnHeapVector(new double[] {1.0})), 1e-4);
    }
}
