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

package org.apache.ignite.ml;

import java.io.Serializable;
import java.util.Arrays;
import org.apache.ignite.ml.math.DistanceMeasure;
import org.apache.ignite.ml.math.Vector;

/** Fuzzy C-Means model representation. */
public class FuzzyCMeansModelFormat implements Serializable {
    /** Centers of clusters. */
    private final Vector[] centers;

    /** Distance measure. */
    private final DistanceMeasure measure;

    /**
     * Constructor that retains result of clusterization and distance measure.
     *
     * @param centers Centers found while clusterization.
     * @param measure Distance measure.
     */
    public FuzzyCMeansModelFormat(Vector[] centers, DistanceMeasure measure) {
        this.centers = centers;
        this.measure = measure;
    }

    /** Distance measure used while clusterization. */
    public DistanceMeasure getMeasure() {
        return measure;
    }

    /** Get cluster centers. */
    public Vector[] getCenters() {
        return centers;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 1;

        res = res * 37 + measure.hashCode();
        res = res * 37 + Arrays.hashCode(centers);

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null || getClass() != obj.getClass())
            return false;

        FuzzyCMeansModelFormat that = (FuzzyCMeansModelFormat) obj;

        return measure.equals(that.measure) && Arrays.deepEquals(centers, that.centers);
    }
}
