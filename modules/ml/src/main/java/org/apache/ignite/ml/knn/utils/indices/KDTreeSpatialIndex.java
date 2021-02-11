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

import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import org.apache.ignite.ml.knn.utils.PointWithDistance;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;

import static org.apache.ignite.ml.knn.utils.PointWithDistanceUtil.transformToListOrdered;
import static org.apache.ignite.ml.knn.utils.PointWithDistanceUtil.tryToAddIntoHeap;

/**
 * KD tree based implementation of {@link SpatialIndex}. Asymptotic runtime complexity of finding {@code k} closest
 * elements is {@code O(log(n)*k)}, but it degrades on high dimensional data.
 *
 * @param <L> Label type.
 */
public class KDTreeSpatialIndex<L> implements SpatialIndex<L> {
    /** Distance measure. */
    private final DistanceMeasure distanceMeasure;

    /** Root node of the KD tree. */
    private TreeNode root;

    /**
     * Constructs a new instance of KD tree spatial index. To construct KD tree a "randomized" approach is uses, all
     * nodes are inserted into the tree sequentially without any additional computations and re-balancing.
     *
     * @param data Data points.
     * @param distanceMeasure Distance measure.
     */
    public KDTreeSpatialIndex(List<LabeledVector<L>> data, DistanceMeasure distanceMeasure) {
        this.distanceMeasure = distanceMeasure;

        data.forEach(dataPnt -> root = add(root, dataPnt));
    }

    /** {@inheritDoc} */
    @Override public List<LabeledVector<L>> findKClosest(int k, Vector pnt) {
        if (k <= 0)
            throw new IllegalArgumentException("Number of neighbours should be positive.");

        Queue<PointWithDistance<L>> heap = new PriorityQueue<>(Collections.reverseOrder());

        findKClosest(pnt, root, 0, heap, k);

        return transformToListOrdered(heap);
    }

    /**
     * Updates collection of closest points processing specified KD tree node.
     *
     * @param pnt Point to calculate distance to.
     * @param node KD tree node.
     * @param splitDim Split dimension that corresponds to current KD tree level.
     * @param heap Heap with closest points.
     * @param k Number of closest points to be collected.
     */
    private void findKClosest(Vector pnt, TreeNode node, int splitDim, Queue<PointWithDistance<L>> heap, int k) {
        if (node == null)
            return;

        tryToAddIntoHeap(heap, k, node.val, distanceMeasure.compute(pnt, node.val.features()));

        double pntPrj = pnt.get(splitDim);
        double splitPrj = node.val.get(splitDim);

        TreeNode primaryBranch = pntPrj > splitPrj ? node.right : node.left;
        TreeNode secondaryBranch = primaryBranch == node.right ? node.left : node.right;

        findKClosestInSplittedSpace(
            pnt,
            primaryBranch,
            secondaryBranch,
            (splitDim + 1) % pnt.size(),
            Math.abs(pntPrj - splitPrj),
            heap,
            k
        );
    }

    /**
     * Updates collection of closest points looking into primary branch and if distance to plane is less then distance
     * to the most distant point within closest point looks into secondary branch as well.
     *
     * @param pnt Point to calculate distance to.
     * @param primaryBrach Primary branch ({@code pnt} belongs to this subtree).
     * @param secondaryBranch Secondary branch ({@code pnt} doesn't belong to this subtree).
     * @param splitDim Split dimension that corresponds to current KD tree level.
     * @param distToPlane Distance to split plane.
     * @param heap Heap with closest points.
     * @param k Number of closest points to be collected.
     */
    private void findKClosestInSplittedSpace(Vector pnt, TreeNode primaryBrach, TreeNode secondaryBranch, int splitDim,
        double distToPlane, Queue<PointWithDistance<L>> heap, int k) {

        findKClosest(pnt, primaryBrach, splitDim, heap, k);

        // If the distance to the most distant element in the heap is less than distance to the plane we need to process
        // the secondary branch as well.
        if (heap.size() < k || distToPlane < heap.peek().getDistance())
            findKClosest(pnt, secondaryBranch, splitDim, heap, k);
    }

    /**
     * Adds element into an existing or not existing KDTree.
     *
     * @param root Root node of KDTree or {@code null}.
     * @param val Value to be added.
     * @return Root node of KDTree.
     */
    private TreeNode add(TreeNode root, LabeledVector<L> val) {
        if (root == null)
            return new TreeNode(val);

        addIntoExistingTree(root, val);

        return root;
    }

    /**
     * Adds element into an existing KD tree.
     *
     * @param node Root node of KD tree.
     * @param pnt Point to be added.
     */
    private void addIntoExistingTree(TreeNode node, LabeledVector<L> pnt) {
        int splitDim = 0;

        while (true) {
            if (pnt.get(splitDim) > node.val.get(splitDim)) {
                if (node.right == null) {
                    node.right = new TreeNode(pnt);
                    break;
                }

                node = node.right;
            }
            else {
                if (node.left == null) {
                    node.left = new TreeNode(pnt);
                    break;
                }

                node = node.left;
            }

            splitDim = (splitDim + 1) % pnt.size();
        }
    }

    /**
     * Binary tree node with {@code val}, {@code left} and {@code right} children.
     */
    private final class TreeNode {
        /** Value. */
        private final LabeledVector<L> val;

        /** Left child. */
        private TreeNode left;

        /** Right child. */
        private TreeNode right;

        /**
         * Constructs a new instance of binary tree node.
         *
         * @param val value.
         */
        TreeNode(LabeledVector<L> val) {
            this.val = val;
        }
    }
}
