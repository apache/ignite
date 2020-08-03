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

package org.apache.ignite.ml.knn.classification;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.knn.KNNModel;
import org.apache.ignite.ml.knn.utils.indices.SpatialIndex;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;

/**
 * KNN classification model. Be aware that this model is linked with cluster environment it's been built on and can't
 * be saved or used in other places. Under the hood it keeps {@link Dataset} that consists of a set of resources
 * allocated across the cluster.
 */
public class KNNClassificationModel extends KNNModel<Double> {
    /**
     * Constructs a new instance of KNN classification model.
     *
     * @param dataset Dataset with {@link SpatialIndex} as a partition data.
     * @param distanceMeasure Distance measure.
     * @param k Number of neighbours.
     * @param weighted Weighted or not.
     */
    KNNClassificationModel(Dataset<EmptyContext, SpatialIndex<Double>> dataset, DistanceMeasure distanceMeasure, int k,
        boolean weighted) {
        super(dataset, distanceMeasure, k, weighted);
    }

    /** {@inheritDoc} */
    @Override public Double predict(Vector input) {
        List<LabeledVector<Double>> neighbors = findKClosest(k, input);

        return election(neighbors, input);
    }

    /**
     * Elects a label with max votes for it.
     *
     * @param neighbours List of neighbours with different labels.
     * @param pnt Point to calculate distance to.
     * @return Label with max votes for it.
     */
    private Double election(List<LabeledVector<Double>> neighbours, Vector pnt) {
        Collection<GroupedNeighbours> groups = groupByLabel(neighbours);

        return election(groups, pnt);
    }

    /**
     * Elects a label with max votes for it.
     *
     * @param groups Groups of neighbours (each group contains neighbours with the same label).
     * @param pnt Point to calculate distance to.
     * @return Label with max votes for it.
     */
    private Double election(Collection<GroupedNeighbours> groups, Vector pnt) {
        Double res = null;
        double votes = 0.0;

        for (GroupedNeighbours groupedNeighbours : groups) {
            double grpVotes = calculateGroupVotes(groupedNeighbours, pnt);
            if (grpVotes > votes) {
                votes = grpVotes;
                res = groupedNeighbours.getLb();
            }
        }

        return res;
    }

    /**
     * Calculate votes of the specific {@link GroupedNeighbours} group.
     *
     * @param grp Groupd of neighbours with the same label.
     * @param pnt Point to calculate distance to.
     * @return Total vote for the label of the given group.
     */
    private Double calculateGroupVotes(GroupedNeighbours grp, Vector pnt) {
        double res = 0;

        for (Vector neighbour : grp) {
            double distance = distanceMeasure.compute(pnt, neighbour);
            double vote = weighted ? 1.0 / distance : 1.0;
            res += vote;
        }

        return res;
    }

    /**
     * Groups given list of neighbours represented by vectors by label on several {@link GroupedNeighbours} groups.
     *
     * @param neighbours List of neighbours.
     * @return Collection of grouped neighbours (each group contains neighbours with the same label).
     */
    private Collection<GroupedNeighbours> groupByLabel(List<LabeledVector<Double>> neighbours) {
        Map<Double, GroupedNeighbours> groups = new HashMap<>();

        for (LabeledVector<Double> neighbour : neighbours) {
            double lb = neighbour.label();

            GroupedNeighbours groupedNeighbours = groups.get(lb);
            if (groupedNeighbours == null) {
                groupedNeighbours = new GroupedNeighbours(lb);
                groups.put(lb, groupedNeighbours);
            }

            groupedNeighbours.addNeighbour(neighbour.features());
        }

        return Collections.unmodifiableCollection(groups.values());
    }

    /**
     * Util class that represents neighbours grouped by label (each group contains neighbours with the same label).
     */
    private static class GroupedNeighbours implements Iterable<Vector> {
        /** Label. */
        private final Double lb;

        /** Neighbours. */
        private final List<Vector> neighbours = new ArrayList<>();

        /**
         * Constructs a new instance of grouped neighbours.
         *
         * @param lb Label.
         */
        public GroupedNeighbours(Double lb) {
            this.lb = lb;
        }

        /**
         * Adds a new neighbour into the group.
         *
         * @param neighbour Neighbour.
         */
        public void addNeighbour(Vector neighbour) {
            neighbours.add(neighbour);
        }

        /**
         * Return label of the group.
         *
         * @return Label of the group.
         */
        public Double getLb() {
            return lb;
        }

        /** {@inheritDoc} */
        @Override public Iterator<Vector> iterator() {
            return neighbours.iterator();
        }
    }
}
