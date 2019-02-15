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

package org.apache.ignite.ml.dataset.feature;

import java.io.Serializable;

/**
 * Bucket meta-information for feature histogram.
 */
public class BucketMeta implements Serializable {
    /** Feature meta. */
    private final FeatureMeta featureMeta;

    /** Bucket size. */
    private double bucketSize;

    /** Min value of feature. */
    private double minVal;

    /**
     * Creates an instance of BucketMeta.
     *
     * @param featureMeta Feature meta.
     */
    public BucketMeta(FeatureMeta featureMeta) {
        this.featureMeta = featureMeta;
    }

    /**
     * Returns bucket id for feature value.
     *
     * @param val Value.
     * @return bucket id.
     */
    public int getBucketId(Double val) {
        if(featureMeta.isCategoricalFeature())
            return (int) Math.rint(val);

        return (int) Math.rint((val - minVal) / bucketSize);
    }

    /**
     * Returns mean value by bucket id.
     *
     * @param bucketId Bucket id.
     * @return mean value of feature.
     */
    public double bucketIdToValue(int bucketId) {
        if(featureMeta.isCategoricalFeature())
            return (double) bucketId;

        return minVal + (bucketId + 0.5) * bucketSize;
    }

    /**
     * @param minVal Min value.
     */
    public void setMinVal(double minVal) {
        this.minVal = minVal;
    }

    /**
     * @param bucketSize Bucket size.
     */
    public void setBucketSize(double bucketSize) {
        this.bucketSize = bucketSize;
    }

    /** */
    public FeatureMeta getFeatureMeta() {
        return featureMeta;
    }
}
