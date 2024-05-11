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

package org.apache.ignite.ml.clustering.kmeans;

import java.io.Serializable;
import java.util.Arrays;
import org.apache.ignite.ml.Exportable;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * K-means model representation.
 *
 * @see Exportable
 * @see Exporter
 */
public class KMeansModelFormat implements Serializable {
    /** Centers of clusters. */
    private final Vector[] centers;

    /** Distance measure. */
    private final DistanceMeasure distance;

    /** */
    public KMeansModelFormat(Vector[] centers, DistanceMeasure distance) {
        this.centers = centers;
        this.distance = distance;
    }

    /** */
    public DistanceMeasure getDistance() {
        return distance;
    }

    /** */
    public Vector[] getCenters() {
        return centers;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 1;

        res = res * 37 + distance.hashCode();
        res = res * 37 + Arrays.hashCode(centers);

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null || getClass() != obj.getClass())
            return false;

        KMeansModelFormat that = (KMeansModelFormat)obj;

        return distance.equals(that.distance) && Arrays.deepEquals(centers, that.centers);
    }

}
