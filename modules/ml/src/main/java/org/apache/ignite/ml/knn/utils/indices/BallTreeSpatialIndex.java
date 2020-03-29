/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.knn.utils.indices;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import org.apache.ignite.ml.knn.utils.PointWithDistance;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.structures.LabeledVector;

import static org.apache.ignite.ml.knn.utils.PointWithDistanceUtil.transformToListOrdered;
import static org.apache.ignite.ml.knn.utils.PointWithDistanceUtil.tryToAddIntoHeap;

/**
 * Ball tree based implementation of {@link SpatialIndex}. Asymptotic runtime complexity of finding {@code k} closest
 * elements is {@code O(log(n)*k)}, but it degrades on high dimensional data.
 *
 * @param <L> Label type.
 */
public class BallTreeSpatialIndex<L> implements SpatialIndex<L> {
    /** Number of points in a leaf. */
    private static final int MAX_LEAF_SIZE = 42;

    /** Margin used to identify center of Balls during data points split. */
    private static final double SPLIT_BALL_MARGIN = 0.2;

    /** Distance measure. */
    private final DistanceMeasure distanceMeasure;

    /** Root node of Ball tree. */
    private final TreeNode root;

    /**
     * Constructs a new instance of Ball tree spatial index.
     *
     * @param data Data.
     * @param distanceMeasure Distance measure.
     */
    public BallTreeSpatialIndex(List<LabeledVector<L>> data, DistanceMeasure distanceMeasure) {
        this.distanceMeasure = distanceMeasure;
        root = buildTree(data);
    }

    /** {@inheritDoc} */
    @Override public List<LabeledVector<L>> findKClosest(int k, Vector pnt) {
        Queue<PointWithDistance<L>> heap = new PriorityQueue<>(Collections.reverseOrder());

        root.findKClosest(pnt, heap, k);

        return transformToListOrdered(heap);
    }

    /**
     * Builds Ball tree.
     *
     * @param data Data points.
     * @return Ball tree root node.
     */
    private TreeNode buildTree(List<LabeledVector<L>> data) {
        Vector center = calculateCenter(data);

        return buildTree(data, center, calculateRadius(data, center));
    }

    /**
     * Builds Ball tree using specified {@code center} and {@code radius} as parameters of current tree node.
     *
     * @param data Data points.
     * @param center Center of the current tree node.
     * @param radius Radius of the current tree node.
     * @return Ball tree node.
     */
    private TreeNode buildTree(List<LabeledVector<L>> data, Vector center, double radius) {
        if (data.size() <= MAX_LEAF_SIZE)
            return new TreeLeafNode(center, radius, data);

        Vector leftCenter = calculateCenter(data);
        Vector rightCenter = leftCenter.copy();

        int bestDimForSplit = calculateBestDimForSplit(data);
        double min = calculateMin(data, bestDimForSplit);
        double max = calculateMax(data, bestDimForSplit);

        leftCenter.set(bestDimForSplit, min + (max - min) * SPLIT_BALL_MARGIN);
        rightCenter.set(bestDimForSplit, min + (max - min) * (1 - SPLIT_BALL_MARGIN));

        List<LabeledVector<L>> leftBallPnts = new ArrayList<>();
        List<LabeledVector<L>> rightBallPnts = new ArrayList<>();

        splitPoints(data, leftCenter, rightCenter, leftBallPnts, rightBallPnts);

        data.clear(); // Help GC to collect unused list.

        return new TreeInnerNode(
            center,
            radius,
            buildTree(leftBallPnts, leftCenter, calculateRadius(leftBallPnts, leftCenter)),
            buildTree(rightBallPnts, rightCenter, calculateRadius(rightBallPnts, rightCenter))
        );
    }

    /**
     * Splits list of data points on two parts: {@code leftBallPnts} and {@code rightBallPnts} so that all points in
     * {@code leftBallPnts} are closer to left center than to right center and all points in {@code rightBallPnts} are
     * closer to right center than to left center.
     *
     * @param dataPnts Data points.
     * @param leftCenter Left center.
     * @param rightCenter Right center.
     * @param leftBallPnts Left ball points (out parameter).
     * @param rightBallPnts Right ball points (out parameter).
     */
    private void splitPoints(List<LabeledVector<L>> dataPnts, Vector leftCenter, Vector rightCenter,
        List<LabeledVector<L>> leftBallPnts, List<LabeledVector<L>> rightBallPnts) {
        for (LabeledVector<L> dataPnt : dataPnts) {
            double distToLeftCenter = distanceMeasure.compute(leftCenter, dataPnt.features());
            double distToRightCenter = distanceMeasure.compute(rightCenter, dataPnt.features());

            List<LabeledVector<L>> targetBallPnts = distToLeftCenter < distToRightCenter ? leftBallPnts : rightBallPnts;
            targetBallPnts.add(dataPnt);
        }
    }

    /**
     * Calculates radius of a ball (max distance from center to data point).
     *
     * @param data Data points.
     * @param center Center of a ball.
     * @return Radius of a ball.
     */
    private double calculateRadius(List<LabeledVector<L>> data, Vector center) {
        double radius = 0;

        for (LabeledVector<L> dataPnt : data) {
            double distance = distanceMeasure.compute(center, dataPnt.features());
            radius = Math.max(radius, distance);
        }

        return radius;
    }

    /**
     * Calculates center of the group of data points using mean values across all dimensions.
     *
     * @param data Data points.
     * @return Center of the group of points.
     */
    private Vector calculateCenter(List<LabeledVector<L>> data) {
        if (data.isEmpty())
            return null;

        double[] center = new double[data.get(0).size()];
        for (int dim = 0; dim < center.length; dim++)
            center[dim] = calculateMean(data, dim);

        return VectorUtils.of(center);
    }

    /**
     * Calculates best dimension for split space on two balls.
     *
     * @param data Data points.
     * @return Dimension.
     */
    private int calculateBestDimForSplit(List<LabeledVector<L>> data) {
        if (data.isEmpty())
            return -1;

        double bestStd = 0;
        int bestDim = -1;

        for (int dim = 0; dim < data.get(0).size(); dim++) {
            double std = calculateStd(data, dim);
            if (std > bestStd) {
                bestStd = std;
                bestDim = dim;
            }
        }

        return bestDim;
    }

    /**
     * Calculates max value for the list of data points and specified dimension.
     *
     * @param data Data points.
     * @param dim Dimension.
     * @return Max value.
     */
    private double calculateMax(List<LabeledVector<L>> data, int dim) {
        double max = Double.NEGATIVE_INFINITY;

        for (LabeledVector<L> dataPnt : data)
            max = Math.max(max, dataPnt.get(dim));

        return max;
    }

    /**
     * Calculates min value for the list of data points and specified dimension.
     *
     * @param data Data points.
     * @param dim Dimension.
     * @return Min value.
     */
    private double calculateMin(List<LabeledVector<L>> data, int dim) {
        double min = Double.POSITIVE_INFINITY;

        for (LabeledVector<L> dataPnt : data)
            min = Math.min(min, dataPnt.get(dim));

        return min;
    }

    /**
     * Calculates standard deviation for the list of data points and specified dimension.
     *
     * @param data Data points.
     * @param dim Dimension.
     * @return Standard deviation.
     */
    private double calculateStd(List<LabeledVector<L>> data, int dim) {
        double res = 0;

        double mean = calculateMean(data, dim);
        for (LabeledVector<L> dataPnt : data)
            res += Math.pow(dataPnt.get(dim) - mean, 2);

        return Math.sqrt(res / data.size());
    }

    /**
     * Calculates mean value for the list of data points and specified dimension.
     *
     * @param data Data points.
     * @param dim Dimension.
     * @return Mean value.
     */
    private double calculateMean(List<LabeledVector<L>> data, int dim) {
        double res = 0;

        for (LabeledVector<L> dataPnt : data)
            res += dataPnt.get(dim);

        return res / data.size();
    }

    /**
     * Ball tree node.
     */
    private abstract class TreeNode {
        /** Center of the ball. */
        private final Vector center;

        /** Radius of the ball. */
        private final double radius;

        /**
         * Constructs a new instance of Ball tree node.
         *
         * @param center Center of the ball.
         * @param radius Radius of the ball.
         */
        TreeNode(Vector center, double radius) {
            this.center = center;
            this.radius = radius;
        }

        /**
         * Finds {@code k} closest elements the the specified point and adds them into {@code heap}.
         *
         * @param pnt Point to be used to calculate distance to other points.
         * @param heap Heap with closest points.
         * @param k Number of closest points to be collected.
         */
        abstract void findKClosest(Vector pnt, Queue<PointWithDistance<L>> heap, int k);

        /** */
        public Vector getCenter() {
            return center;
        }

        /** */
        public double getRadius() {
            return radius;
        }
    }

    /**
     * Inner node of Ball tree that contains two children nodes.
     */
    private final class TreeInnerNode extends TreeNode {
        /** Left child node. */
        private final TreeNode left;

        /** Right child node. */
        private final TreeNode right;

        /**
         * Constructs a new instance of Ball tree inner node.
         *
         * @param center Center of the ball.
         * @param radius Radius of the ball.
         */
        TreeInnerNode(Vector center, double radius, TreeNode left, TreeNode right) {
            super(center, radius);
            this.left = left;
            this.right = right;
        }

        /** {@inheritDoc} */
        @Override void findKClosest(Vector pnt, Queue<PointWithDistance<L>> heap, int k) {
            double distToLeftCenter = computeDistToCenter(pnt, left);
            double distToRightCenter = computeDistToCenter(pnt, right);

            TreeNode primaryBranch = distToLeftCenter > distToRightCenter ? right : left;
            TreeNode secondaryBranch = primaryBranch == right ? left : right;

            if (primaryBranch != null)
                primaryBranch.findKClosest(pnt, heap, k);

            // If the distance to the most distant element in the heap is less than distance to the plane we need to process
            // the secondary branch as well.
            if (secondaryBranch != null) {
                double distToSecondaryBall = computeDistToCenter(pnt, secondaryBranch) - secondaryBranch.getRadius();
                if (heap.size() < k || distToSecondaryBall < heap.peek().getDistance())
                    secondaryBranch.findKClosest(pnt, heap, k);
            }
        }

        /**
         * Computed distance from point to center of Ball tree node.
         *
         * @param pnt Point to be used to calculate distance to other points.
         * @param node Ball tree node.
         * @return Distance from point to center of Ball tree node.
         */
        private double computeDistToCenter(Vector pnt, TreeNode node) {
            if (node == null)
                return Double.MAX_VALUE;

            return distanceMeasure.compute(pnt, node.getCenter());
        }
    }

    /**
     * Leaf node of Ball tree that contains an array of points that owned by the leaf.
     */
    private final class TreeLeafNode extends TreeNode {
        /** Array of points owned by the leaf. */
        private final List<LabeledVector<L>> points;

        /**
         * Constructs a new instance of Ball tree leaf node.
         *
         * @param center Center of the ball.
         * @param radius Radius of the ball.
         * @param points List of points owned by the leaf.
         */
        TreeLeafNode(Vector center, double radius, List<LabeledVector<L>> points) {
            super(center, radius);
            this.points = points;
        }

        /** {@inheritDoc} */
        @Override void findKClosest(Vector pnt, Queue<PointWithDistance<L>> heap, int k) {
            for (LabeledVector<L> dataPnt : points) {
                double distance = distanceMeasure.compute(pnt, dataPnt.features());
                tryToAddIntoHeap(heap, k, dataPnt, distance);
            }
        }
    }
}
