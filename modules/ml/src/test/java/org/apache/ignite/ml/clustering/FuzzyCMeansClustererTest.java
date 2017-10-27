package org.apache.ignite.ml.clustering;

import org.apache.ignite.ml.math.DistanceMeasure;
import org.apache.ignite.ml.math.EuclideanDistance;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;

import static org.junit.Assert.*;

public class FuzzyCMeansClustererTest {
    @Test
    public void equalWeightsOneDimension() {
        BaseFuzzyCMeansClusterer clusterer = new FuzzyCMeansLocalClusterer(new EuclideanDistance(),
                2, 0.01, 10, null);

        double[][] points = new double[][]{{-10}, {-9}, {-8}, {-7},
                                           {7},   {8},  {9},  {10},
                                           {-1},  {0},  {1}};

        Matrix pointMatrix = new DenseLocalOnHeapMatrix(points);

        FuzzyCMeansModel model = clusterer.cluster(pointMatrix, 3);

        Vector[] centers = model.centers();
        Arrays.sort(centers, Comparator.comparing(vector -> vector.getX(0)));
        assertEquals(-8.5, centers[0].getX(0), 2);
        assertEquals(0, centers[1].getX(0), 2);
        assertEquals(8.5, centers[2].getX(0), 2);
    }

    @Test
    public void equalWeightsTwoDimensions() {
        BaseFuzzyCMeansClusterer clusterer = new FuzzyCMeansLocalClusterer(new EuclideanDistance(),
                2, 0.01, 10, null);

        double[][] points = new double[][]{{-10, -10}, {-9, -11}, {-10, -9}, {-11, -9},
                                           {10, 10},   {9, 11},   {10, 9},   {11, 9},
                                           {-10, 10},  {-9, 11},  {-10, 9},  {-11, 9},
                                           {10, -10},  {9, -11},  {10, -9},  {11, -9}};

        Matrix pointMatrix = new DenseLocalOnHeapMatrix(points);

        FuzzyCMeansModel model = clusterer.cluster(pointMatrix, 4);

        for (int i = 0; i < 4; i++) {
            Vector center = model.centers()[i];
            System.out.print(center.getX(0));
            System.out.print(' ');
            System.out.println(center.getX(1));
        }
    }

    @Test
    public void correctOneCentreOfSimilarPoints() {
        BaseFuzzyCMeansClusterer clusterer = new FuzzyCMeansLocalClusterer(new EuclideanDistance(),
                2, 0.01, 10, null);

        double[][] points = new double[][] {{3.3}, {3.3}, {3.3}, {3,3}};

        Matrix pointMatrix = new DenseLocalOnHeapMatrix(points);

        FuzzyCMeansModel model = clusterer.cluster(pointMatrix, 1);
        Vector[] centers = model.centers();
        assertEquals(3.3, centers[0].getX(0), 2);
    }

    @Test
    public void centresOfSimilarPoints() {
        BaseFuzzyCMeansClusterer clusterer = new FuzzyCMeansLocalClusterer(new EuclideanDistance(),
                2, 0.01, 10, null);

        double[][] points = new double[][] {{3.3, 10}, {3.3, 10}, {3.3, 10}, {3.3, 10}, {3.3, 10}};

        Matrix pointMatrix = new DenseLocalOnHeapMatrix(points);

        int k = 2;
        FuzzyCMeansModel model = clusterer.cluster(pointMatrix, k);
        Vector[] centers = model.centers();
        for (int i = 0; i < k; i++) {
            Vector center = model.centers()[i];
            System.out.print(center.getX(0));
            System.out.print(' ');
            System.out.println(center.getX(1));
        }
    }

    @Test
    public void twoCenters() {
        BaseFuzzyCMeansClusterer clusterer = new FuzzyCMeansLocalClusterer(new EuclideanDistance(),
                2, 0.01, 10, null);

        double[][] points = new double[][] {{10, 10}, {10, -10}, {-10, -10}, {-10, 10}};

        Matrix pointMatrix = new DenseLocalOnHeapMatrix(points);

        int k = 2;
        FuzzyCMeansModel model = clusterer.cluster(pointMatrix, k);
        for (int i = 0; i < k; i++) {
            Vector center = model.centers()[i];
            System.out.print(center.getX(0));
            System.out.print(' ');
            System.out.println(center.getX(1));
        }
    }
}
