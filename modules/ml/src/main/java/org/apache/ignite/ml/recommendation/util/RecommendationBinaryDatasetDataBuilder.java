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

package org.apache.ignite.ml.recommendation.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.recommendation.ObjectSubjectRatingTriplet;

/**
 * Recommendation binary dataset data builder.
 */
public class RecommendationBinaryDatasetDataBuilder
    implements PartitionDataBuilder<Object, BinaryObject, EmptyContext, RecommendationDatasetData<Serializable,
    Serializable>> {
    /** */
    private static final long serialVersionUID = 7807817562932691037L;

    /** Object field name. */
    private final String objFieldName;

    /** Subject field name. */
    private final String subjFieldName;

    /** Rating field name. */
    private final String ratingFieldName;

    /**
     * Constructs a new instance of recommendation binary dataset data builder.
     *
     * @param objFieldName Object field name.
     * @param subjFieldName Subject field name.
     * @param ratingFieldName Rating field name.
     */
    public RecommendationBinaryDatasetDataBuilder(String objFieldName, String subjFieldName, String ratingFieldName) {
        this.objFieldName = objFieldName;
        this.subjFieldName = subjFieldName;
        this.ratingFieldName = ratingFieldName;
    }

    /** {@inheritDoc} */
    @Override public RecommendationDatasetData<Serializable, Serializable> build(LearningEnvironment env,
        Iterator<UpstreamEntry<Object, BinaryObject>> upstreamData, long upstreamDataSize, EmptyContext ctx) {
        List<ObjectSubjectRatingTriplet<Serializable, Serializable>> ratings = new ArrayList<>();
        while (upstreamData.hasNext()) {
            UpstreamEntry<Object, BinaryObject> e = upstreamData.next();
            BinaryObject val = e.getValue();

            Serializable objId = val.field(objFieldName);
            Serializable subjId = val.field(subjFieldName);
            Number rating = val.field(ratingFieldName);

            ratings.add(new ObjectSubjectRatingTriplet<>(objId, subjId, rating.doubleValue()));
        }

        return new RecommendationDatasetData<>(ratings);
    }
}
