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
import org.apache.ignite.ml.Exportable;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.FuzzyCMeansModelFormat;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.distances.DistanceMeasure;

/** This class incapsulates result of clusterization. */
public class FuzzyCMeansModel implements ClusterizationModel<Vector, Integer>, Exportable<FuzzyCMeansModelFormat> {
    /** Centers of clusters. */
    private Vector[] centers;

    /** Distance measure. */
    private DistanceMeasure measure;

    /**
     * Constructor that creates FCM model by centers and measure.
     *
     * @param centers Array of centers.
     * @param measure Distance measure.
     */
    public FuzzyCMeansModel(Vector[] centers, DistanceMeasure measure) {
        this.centers = Arrays.copyOf(centers, centers.length);
        this.measure = measure;
    }

    /** Distance measure used while clusterization. */
    public DistanceMeasure distanceMeasure() {
        return measure;
    }

    /** @inheritDoc */
    @Override public int clustersCount() {
        return centers.length;
    }

    /** @inheritDoc */
    @Override public Vector[] centers() {
        return Arrays.copyOf(centers, centers.length);
    }

    /**
     * Predict closest center index for a given vector.
     *
     * @param val Vector.
     * @return Index of the closest center or -1 if it can't be found.
     */
    @Override public Integer predict(Vector val) {
        int idx = -1;
        double minDistance = Double.POSITIVE_INFINITY;

        for (int i = 0; i < centers.length; i++) {
            double currDistance = measure.compute(val, centers[i]);
            if (currDistance < minDistance) {
                minDistance = currDistance;
                idx = i;
            }
        }

        return idx;
    }

    /** {@inheritDoc} */
    @Override public <P> void saveModel(Exporter<FuzzyCMeansModelFormat, P> exporter, P path) {
        FuzzyCMeansModelFormat mdlData = new FuzzyCMeansModelFormat(centers, measure);

        exporter.save(mdlData, path);
    }
}
