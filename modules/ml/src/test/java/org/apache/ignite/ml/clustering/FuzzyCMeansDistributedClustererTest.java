package org.apache.ignite.ml.clustering;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.math.EuclideanDistance;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.matrix.SparseDistributedMatrix;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import java.util.Random;

/**
 * Created by kroonk on 27.10.17.
 */
public class FuzzyCMeansDistributedClustererTest extends GridCommonAbstractTest {
    private static final int NODE_COUNT = 1;

    /** Grid instance. */
    private Ignite ignite;

    /**
     * Default constructor.
     */
    public FuzzyCMeansDistributedClustererTest() {
        super(false);
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        ignite = grid(NODE_COUNT);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 1; i <= NODE_COUNT; i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    @Test
    public void testTwoDimensionsLittleData() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        BaseFuzzyCMeansClusterer clusterer = new FuzzyCMeansDistributedClusterer(new EuclideanDistance(),
                2, 0.01, null, 2, 50);

        double[][] points = new double[][]{{-10, -10}, {-9, -11}, {-10, -9}, {-11, -9},
                {10, 10},   {9, 11},   {10, 9},   {11, 9},
                {-10, 10},  {-9, 11},  {-10, 9},  {-11, 9},
                {10, -10},  {9, -11},  {10, -9},  {11, -9}};

        Matrix pointMatrix = new SparseDistributedMatrix(16, 2,
                StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);
        for (int i = 0; i < 16; i++) {
            pointMatrix.setRow(i, points[i]);
        }

        FuzzyCMeansModel model = clusterer.cluster(pointMatrix, 4);

        for (int i = 0; i < 4; i++) {
            Vector center = model.centers()[i];
            System.out.print(center.getX(0));
            System.out.print(' ');
            System.out.println(center.getX(1));
        }
    }

    public void testTwoDimensionsLargeData() {
        Random random = new Random();

        final int numCenters = 4;
        final int pointsAroundCenters = 2000;

        double[][] centers = new double[numCenters][2];
        for (int i = 0; i < numCenters; i++) {
            centers[i][0] = Math.cos(Math.PI * 2 / numCenters * i) * 100000;
            centers[i][1] = Math.sin(Math.PI * 2 / numCenters * i) * 100000;
        }

        double[][] points = new double[numCenters * pointsAroundCenters][2];
        for (int i = 0; i < numCenters; i++) {
            for (int j = 0; j < pointsAroundCenters; j++) {
                int id = i * pointsAroundCenters + j;
                points[id][0] = random.nextDouble() * 20000 + centers[i][0];
                points[id][1] = random.nextDouble() * 20000 + centers[i][1];
            }
        }

        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());

        Matrix pointMatrix = new SparseDistributedMatrix(numCenters * pointsAroundCenters, 2,
                StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);
        for (int i = 0; i < numCenters * pointsAroundCenters; i++) {
            pointMatrix.setRow(i, points[i]);
        }

        BaseFuzzyCMeansClusterer clusterer = new FuzzyCMeansDistributedClusterer(new EuclideanDistance(),
                2, 0.01, null, 2, 50);

        FuzzyCMeansModel model = clusterer.cluster(pointMatrix, numCenters);

        for (int i = 0; i < numCenters; i++) {
            Vector center = model.centers()[i];
            System.out.print((int) center.get(0));
            System.out.print(' ');
            System.out.println((int) center.get(1));
        }
    }
}
