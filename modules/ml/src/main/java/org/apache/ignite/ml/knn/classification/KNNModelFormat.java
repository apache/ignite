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

import java.io.Serializable;
import org.apache.ignite.ml.math.distances.DistanceMeasure;

/**
 * kNN model representation.
 *
 * @see KNNClassificationModel
 */
public class KNNModelFormat implements Serializable {
    /** Amount of nearest neighbors. */
    protected int k;

    /** Distance measure. */
    protected DistanceMeasure distanceMeasure;

    /** kNN strategy. */
    protected NNStrategy stgy;

    /** Gets amount of nearest neighbors.*/
    public int getK() {
        return k;
    }

    /** Gets distance measure. */
    public DistanceMeasure getDistanceMeasure() {
        return distanceMeasure;
    }

    /** Gets kNN strategy.*/
    public NNStrategy getStgy() {
        return stgy;
    }

    /** */
    public KNNModelFormat() {
    }

    /**
     * Creates an instance.
     * @param k Amount of nearest neighbors.
     * @param measure Distance measure.
     * @param stgy kNN strategy.
     */
    public KNNModelFormat(int k, DistanceMeasure measure, NNStrategy stgy) {
        this.k = k;
        this.distanceMeasure = measure;
        this.stgy = stgy;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 1;

        res = res * 37 + k;
        res = res * 37 + distanceMeasure.hashCode();
        res = res * 37 + stgy.hashCode();

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null || getClass() != obj.getClass())
            return false;

        KNNModelFormat that = (KNNModelFormat)obj;

        return k == that.k && distanceMeasure.equals(that.distanceMeasure) && stgy.equals(that.stgy);
    }
}
