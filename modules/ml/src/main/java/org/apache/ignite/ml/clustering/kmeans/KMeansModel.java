/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.ml.clustering.kmeans;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.ml.Exportable;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.math.Tracer;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.util.ModelTrace;

/**
 * This class encapsulates result of clusterization by KMeans algorithm.
 */
public class KMeansModel implements ClusterizationModel<Vector, Integer>, Exportable<KMeansModelFormat> {
    /** Centers of clusters. */
    private final Vector[] centers;

    /** Distance measure. */
    private final DistanceMeasure distanceMeasure;

    /**
     * Construct KMeans model with given centers and distanceMeasure measure.
     *
     * @param centers Centers.
     * @param distanceMeasure Distance measure.
     */
    public KMeansModel(Vector[] centers, DistanceMeasure distanceMeasure) {
        this.centers = centers;
        this.distanceMeasure = distanceMeasure;
    }

    /** Distance measure. */
    public DistanceMeasure distanceMeasure() {
        return distanceMeasure;
    }

    /** Amount of centers in clusterization. */
    @Override public int getAmountOfClusters() {
        return centers.length;
    }

    /** Get centers of clusters. */
    @Override public Vector[] getCenters() {
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
            double curDist = distanceMeasure.compute(centers[i], vec);
            if (curDist < minDist) {
                minDist = curDist;
                res = i;
            }
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public <P> void saveModel(Exporter<KMeansModelFormat, P> exporter, P path) {
        KMeansModelFormat mdlData = new KMeansModelFormat(centers, distanceMeasure);

        exporter.save(mdlData, path);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 1;

        res = res * 37 + distanceMeasure.hashCode();
        res = res * 37 + Arrays.hashCode(centers);

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null || getClass() != obj.getClass())
            return false;

        KMeansModel that = (KMeansModel)obj;

        return distanceMeasure.equals(that.distanceMeasure) && Arrays.deepEquals(centers, that.centers);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return toString(false);
    }

    /** {@inheritDoc} */
    @Override public String toString(boolean pretty) {
        String measureName = distanceMeasure.getClass().getSimpleName();
        List<String> centersList = Arrays.stream(centers).map(x -> Tracer.asAscii(x, "%.4f", false))
            .collect(Collectors.toList());

        return ModelTrace.builder("KMeansModel", pretty)
            .addField("distance measure", measureName)
            .addField("centroids", centersList)
            .toString();
    }
}
