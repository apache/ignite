/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkore.ignite.lucene.search.condition.builder;

import org.hawkore.ignite.lucene.common.Builder;
import org.hawkore.ignite.lucene.search.condition.Condition;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
 
/**
 * {@link Builder} for creating new {@link Condition}s.
 *
 * @param <T> The type of the {@link Condition} to be created.
 * @param <K> The specific type of the {@link ConditionBuilder}.
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type", defaultImpl = BooleanConditionBuilder.class)
@JsonSubTypes({@JsonSubTypes.Type(value = AllConditionBuilder.class, name = "all"),
               @JsonSubTypes.Type(value = BitemporalConditionBuilder.class, name = "bitemporal"),
               @JsonSubTypes.Type(value = BooleanConditionBuilder.class, name = "boolean"),
               @JsonSubTypes.Type(value = ContainsConditionBuilder.class, name = "contains"),
               @JsonSubTypes.Type(value = DateRangeConditionBuilder.class, name = "date_range"),
               @JsonSubTypes.Type(value = FuzzyConditionBuilder.class, name = "fuzzy"),
               @JsonSubTypes.Type(value = GeoDistanceConditionBuilder.class, name = "geo_distance"),
               @JsonSubTypes.Type(value = GeoBBoxConditionBuilder.class, name = "geo_bbox"),
               @JsonSubTypes.Type(value = GeoShapeConditionBuilder.class, name = "geo_shape"),
               @JsonSubTypes.Type(value = LuceneConditionBuilder.class, name = "lucene"),
               @JsonSubTypes.Type(value = MatchConditionBuilder.class, name = "match"),
               @JsonSubTypes.Type(value = NoneConditionBuilder.class, name = "none"),
               @JsonSubTypes.Type(value = PhraseConditionBuilder.class, name = "phrase"),
               @JsonSubTypes.Type(value = PrefixConditionBuilder.class, name = "prefix"),
               @JsonSubTypes.Type(value = RangeConditionBuilder.class, name = "range"),
               @JsonSubTypes.Type(value = RegexpConditionBuilder.class, name = "regexp"),
               @JsonSubTypes.Type(value = WildcardConditionBuilder.class, name = "wildcard")})
public abstract class ConditionBuilder<T extends Condition, K extends ConditionBuilder<T, K>> implements Builder<T> {

    /** The boost for the {@link Condition} to be built. */
    @JsonProperty("boost")
    Float boost;

    /**
     * Sets the boost for the {@link Condition} to be built. Documents matching this condition will (in addition to the
     * normal weightings) have their score multiplied by {@code boost}.
     *
     * @param boost the boost
     * @return this with the specified boost
     */
    @SuppressWarnings("unchecked")
    public K boost(float boost) {
        this.boost = boost;
        return (K) this;
    }

    /**
     * Sets the boost for the {@link Condition} to be built. Documents matching this condition will (in addition to the
     * normal weightings) have their score multiplied by {@code boost}.
     *
     * @param boost he boost
     * @return this with the specified boost
     */
    @SuppressWarnings("unchecked")
    public K boost(Number boost) {
        this.boost = boost == null ? null : boost.floatValue();
        return (K) this;
    }

    /**
     * Returns the {@link Condition} represented by this builder.
     *
     * @return a new condition
     */
    @Override
    public abstract T build();
}
