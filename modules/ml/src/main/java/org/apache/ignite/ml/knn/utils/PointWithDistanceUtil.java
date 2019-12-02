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

package org.apache.ignite.ml.knn.utils;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;

/**
 * Util class with method that help working with {@link PointWithDistance}.
 */
public class PointWithDistanceUtil {
    /**
     * Util method that transforms collection of {@link PointWithDistance} to array of {@link LabeledVector}. Be aware
     * that this method uses {@link Queue#remove()} method to extract elements and the asymptotic complexity of this
     * method depends on it (in case of {@link PriorityQueue} it will be {@code O(log(n))}, in case of
     * {@link LinkedList} it will be {@code O(n)}).
     *
     * @param points Collections of {@link PointWithDistance}.
     * @param <L> Label type.
     * @return Array of {@link LabeledVector}
     */
    public static <L> List<LabeledVector<L>> transformToListOrdered(Queue<PointWithDistance<L>> points) {
        List<LabeledVector<L>> res = new ArrayList<>(points.size());

        while (!points.isEmpty()) {
            PointWithDistance<L> pnt = points.remove();
            res.add(pnt.getPnt());
        }

        return res;
    }

    /**
     * Util method that adds data point into heap if it fits (if heap size is less than {@code k} or a distance from
     * taget point to data point is less than a distance from target point to the most distant data point in heap).
     *
     * @param heap Heap with closest points.
     * @param k Number of neighbours.
     * @param dataPnt Data point to be added.
     * @param distance Distance to target point.
     * @param <L> Label type.
     */
    public static <L> void tryToAddIntoHeap(Queue<PointWithDistance<L>> heap, int k, LabeledVector<L> dataPnt,
        double distance) {
        if (dataPnt != null) {
            if (heap.size() == k && heap.peek().getDistance() > distance)
                heap.remove();

            if (heap.size() < k)
                heap.add(new PointWithDistance<>(dataPnt, distance));
        }
    }

    /**
     * Util method that adds data points into heap if they fits (if heap size is less than {@code k} or a distance from
     * taget point to data point is less than a distance from target point to the most distant data point in heap).
     *
     * @param heap Heap with closest points.
     * @param k Number of neighbours.
     * @param pnt Point to calculate distance to.
     * @param dataPnts Data points to be added.
     * @param distanceMeasure Distance measure.
     * @param <L> Label type.
     */
    public static <L> void tryToAddIntoHeap(Queue<PointWithDistance<L>> heap, int k, Vector pnt,
        List<LabeledVector<L>> dataPnts, DistanceMeasure distanceMeasure) {
        if (dataPnts != null) {
            for (LabeledVector<L> dataPnt : dataPnts) {
                double distance = distanceMeasure.compute(pnt, dataPnt.features());
                tryToAddIntoHeap(heap, k, dataPnt, distance);
            }
        }
    }
}
