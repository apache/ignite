package org.apache.ignite.ml.clustering;

import org.apache.ignite.ml.math.DistanceMeasure;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.exceptions.ConvergenceException;
import org.apache.ignite.ml.math.exceptions.MathIllegalArgumentException;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class FuzzyCMeansLocalClusterer extends BaseFuzzyCMeansClusterer<DenseLocalOnHeapMatrix> implements
        WeightedClusterer<DenseLocalOnHeapMatrix, FuzzyCMeansModel> {

    private int maxIterations;

    private Random rand;

    public FuzzyCMeansLocalClusterer(DistanceMeasure measure, double exponentialWeight,
                                     double maxCentersDelta, int maxIterations, Long seed) {
        super(measure, exponentialWeight, maxCentersDelta);
        this.maxIterations = maxIterations;
        rand = seed != null ? new Random(seed) : new Random();
    }

    @Override
    public FuzzyCMeansModel cluster(DenseLocalOnHeapMatrix points, int k) {
        List<Double> ones = new ArrayList<>(Collections.nCopies(points.rowSize(), 1.0));
        return cluster(points, k, ones);
    }

    @Override
    public FuzzyCMeansModel cluster(DenseLocalOnHeapMatrix points,
                                    int k, List<Double> weights) throws MathIllegalArgumentException, ConvergenceException {
        Matrix centers = new DenseLocalOnHeapMatrix(k, points.columnSize());
        Matrix distances = new DenseLocalOnHeapMatrix(k, points.rowSize());
        Matrix membership = new DenseLocalOnHeapMatrix(k, points.rowSize());
        Vector weightsVector = new DenseLocalOnHeapVector(weights.size());
        for (int i = 0; i < weights.size(); i++) {
            weightsVector.setX(i, weights.get(i));
        }

        initializeCenters(centers, points, k, weightsVector);

        int iteration = 0;
        boolean finished = false;
        while (iteration < maxIterations && !finished) {
            calculateDistances(distances, points, centers);
            calculateMembership(membership, distances, weightsVector);
            Matrix newCenters = calculateNewCenters(points, membership);

            finished = isFinished(centers, newCenters);
            centers = newCenters;
            iteration++;
        }

        Vector[] centersArray = new Vector[k];
        for (int i = 0; i < k; i++) {
            centersArray[i] = centers.getRow(i);
        }
        return new FuzzyCMeansModel(centersArray, measure);
    }

    private Matrix initializeCenters(Matrix centers, Matrix points, int k, Vector weights) {
        //int dimensions = points.columnSize();
        int numPoints = points.rowSize();

        Vector firstCenter = points.viewRow(rand.nextInt(numPoints));
        centers.setRow(0, firstCenter.getStorage().data());

        Vector costs = points.foldRows(vector -> distance(vector, firstCenter));
        costs = costs.times(weights);

        double sum = costs.sum();

        for (int i = 1; i < k; i++) {
            double probe = rand.nextDouble() * sum;
            double counter = 0;
            int id = 0;

            for (int j = 0; j < numPoints; j++) {
                counter += costs.getX(j);
                if (counter >= probe) {
                    id = j;
                    break;
                }
            }

            centers.setRow(i, points.viewRow(id).getStorage().data());
            sum -= costs.get(id);
            costs.set(id, 0.0);
        }

        return centers;
    }

    private void calculateDistances(Matrix distances, Matrix points, Matrix centers) {
        int numPoints = points.rowSize();
        int numCenters = centers.rowSize();

        for (int i = 0; i < numCenters; i++) {
            for (int j = 0; j < numPoints; j++) {
                distances.set(i, j, distance(centers.viewRow(i), points.viewRow(j)));
            }
        }
    }

    private void calculateMembership(Matrix membership, Matrix distances, Vector weights) {
        int numPoints = distances.columnSize();
        int numCenters = distances.rowSize();
        double fuzzyMembershipCoefficient = 2 / (exponentialWeight - 1);

        for (int i = 0; i < numCenters; i++) {
            for (int j = 0; j < numPoints; j++) {
                double invertedFuzzyWeight = 0;
                for (int k = 0; k < numCenters; k++) {
                    double value = Math.pow(distances.get(i, j) / distances.get(k, j),
                            fuzzyMembershipCoefficient);
                    if (Double.isNaN(value)) {
                        value = 1.0;
                    }
                    invertedFuzzyWeight += value;
                }
                double weight = 1.0 / invertedFuzzyWeight * weights.getX(j);
                membership.setX(i, j, Math.pow(weight, exponentialWeight));
            }
        }
    }

    private Matrix calculateNewCenters(Matrix points, Matrix membership) {
        Vector membershipSums = membership.foldRows(row -> row.sum());
        Matrix newCenters = membership.times(points);

        int numCenters = newCenters.rowSize();
        for (int i = 0; i < numCenters; i++) {
            newCenters.viewRow(i).divide(membershipSums.getX(i));
        }

        return newCenters;
    }

    private boolean isFinished(Matrix centers, Matrix newCenters) {
        int numCenters = centers.rowSize();

        for (int i = 0; i < numCenters; i++) {
            if (distance(centers.viewRow(i), newCenters.viewRow(i)) > maxCentersDelta) {
                return false;
            }
        }

        return true;
    }
}
