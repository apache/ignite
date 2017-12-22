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

package org.apache.ignite.ml.knn.models;

import java.io.Serializable;
import java.util.Arrays;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.structures.LabeledDataset;

/**
 * kNN model representation.
 *
 * @see KNNModel
 */
public class KNNModelFormat implements Serializable {
    /** Amount of nearest neighbors. */
    private int k;

    /** Distance measure. */
    private DistanceMeasure distanceMeasure;

    /** Training dataset */
    private LabeledDataset training;

    /** kNN strategy. */
    private KNNStrategy stgy;

    /** */
    public int getK() {
        return k;
    }

    /** */
    public DistanceMeasure getDistanceMeasure() {
        return distanceMeasure;
    }

    /** */
    public LabeledDataset getTraining() {
        return training;
    }

    /** */
    public KNNStrategy getStgy() {
        return stgy;
    }

    /** */
    public KNNModelFormat(int k, DistanceMeasure measure, LabeledDataset training, KNNStrategy stgy) {
        this.k = k;
        this.distanceMeasure = measure;
        this.training = training;
        this.stgy = stgy;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 1;

        res = res * 37 + k;
        res = res * 37 + distanceMeasure.hashCode();
        res = res * 37 + stgy.hashCode();
        res = res * 37 + Arrays.hashCode(training.data());

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null || getClass() != obj.getClass())
            return false;

        KNNModelFormat that = (KNNModelFormat)obj;

        return k == that.k && distanceMeasure.equals(that.distanceMeasure) && stgy.equals(that.stgy)
            && Arrays.deepEquals(training.data(), that.training.data());
    }
}
