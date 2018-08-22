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

package org.apache.ignite.ml.tree.randomforest.data.histogram;

public class BucketMeta {
    private final FeatureMeta featureMeta;
    private double bucketSize;
    private double minValue;

    public BucketMeta(FeatureMeta featureMeta) {
        this.featureMeta = featureMeta;
    }

    public int getBucketId(Double value) {
        if(featureMeta.isCategoricalFeature())
            return (int) Math.rint(value);

        return (int) Math.rint((value - minValue) / bucketSize);
    }

    public double bucketIdToValue(int bucketId) {
        if(featureMeta.isCategoricalFeature())
            return (double) bucketId;

        return minValue + (bucketId + 0.5) * bucketSize;
    }

    public void setMinValue(double minValue) {
        this.minValue = minValue;
    }

    public void setBucketSize(double bucketSize) {
        this.bucketSize = bucketSize;
    }

    public FeatureMeta getFeatureMeta() {
        return featureMeta;
    }
}
