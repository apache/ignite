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
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.dataset.primitive.data.SimpleDatasetData;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.recommendation.ObjectSubjectRatingTriplet;

/**
 * A partition {@code data} builder that makes {@link SimpleDatasetData}.
 *
 * @param <K> Type of a key in <tt>upstream</tt> data.
 * @param <O> Type of an object of recommendation.
 * @param <S> Type of a subject of recommendation.
 * @param <Z> Type of object-subject pair.
 */
public class RecommendationDatasetDataBuilder<K, O extends Serializable, S extends Serializable,
    Z extends ObjectSubjectRatingTriplet<O, S>>
    implements PartitionDataBuilder<K, Z, EmptyContext, RecommendationDatasetData<O, S>> {
    /** */
    private static final long serialVersionUID = -4781371834972484668L;

    /** {@inheritDoc} */
    @Override public RecommendationDatasetData<O, S> build(LearningEnvironment env,
        Iterator<UpstreamEntry<K, Z>> upstreamData, long upstreamDataSize, EmptyContext ctx) {

        List<ObjectSubjectRatingTriplet<O, S>> ratings = new ArrayList<>();
        while (upstreamData.hasNext()) {
            UpstreamEntry<K, Z> e = upstreamData.next();
            ratings.add(e.getValue());
        }

        return new RecommendationDatasetData<>(ratings);
    }
}
