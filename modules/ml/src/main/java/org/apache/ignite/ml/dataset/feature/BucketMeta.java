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

package org.apache.ignite.ml.dataset.feature;

import java.io.Serializable;

/**
 * Bucket meta-information for feature histogram.
 */
public class BucketMeta implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 7827158624437006995L;

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
     * @return Bucket id.
     */
    public int getBucketId(Double val) {
        if (featureMeta.isCategoricalFeature())
            return (int) Math.rint(val);

        return (int) Math.rint((val - minVal) / bucketSize);
    }

    /**
     * Returns mean value by bucket id.
     *
     * @param bucketId Bucket id.
     * @return Mean value of feature.
     */
    public double bucketIdToValue(int bucketId) {
        if (featureMeta.isCategoricalFeature())
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
