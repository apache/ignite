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

package org.apache.ignite.ml.clustering;

import java.util.Arrays;
import org.apache.ignite.ml.math.DistanceMeasure;
import org.apache.ignite.ml.math.Vector;

/**
 * This class encapsulates result of clusterization.
 */
public class KMeansModel implements ClusterizationModel<Vector, Integer> {
    /** Centers of clusters. */
    private Vector[] centers;

    /** Distance measure. */
    private DistanceMeasure distance;

    /**
     * Construct KMeans model with given centers and distance measure.
     *
     * @param centers Centers.
     * @param distance Distance measure.
     */
    KMeansModel(Vector[] centers, DistanceMeasure distance) {
        this.centers = centers;
        this.distance = distance;
    }

    /** Distance measure used while clusterization */
    public DistanceMeasure distanceMeasure() {
        return distance;
    }

    /** Count of centers in clusterization. */
    @Override public int clustersCount() {
        return centers.length;
    }

    /** Get centers of clusters. */
    @Override public Vector[] centers() {
        return Arrays.copyOf(centers, centers.length);
    }

    /**
     * Predict closest center index for a given vector.
     *
     * @param vec Vector.
     */
    public Integer predict(Vector vec) {
        int res = -1;
        double minDist = Double.POSITIVE_INFINITY;

        for (int i = 0; i < centers.length; i++) {
            double curDist = distance.compute(centers[i], vec);
            if (curDist < minDist) {
                minDist = curDist;
                res = i;
            }
        }

        return res;
    }
}
