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
package org.hawkore.ignite.lucene.builder.search.condition;

import org.hawkore.ignite.lucene.builder.JSONBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * The abstract base class for queries.
 *
 * Known subclasses are: <ul> <li> {@link AllCondition} <li> {@link BitemporalCondition} <li> {@link ContainsCondition}
 * <li> {@link FuzzyCondition} <li> {@link MatchCondition} <li> {@link PhraseCondition} <li> {@link PrefixCondition}
 * <li> {@link RangeCondition} <li> {@link WildcardCondition} <li> {@link GeoDistanceCondition} <li> {@link
 * GeoBBoxCondition} </ul>
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({@JsonSubTypes.Type(value = AllCondition.class, name = "all"),
               @JsonSubTypes.Type(value = BitemporalCondition.class, name = "bitemporal"),
               @JsonSubTypes.Type(value = BooleanCondition.class, name = "boolean"),
               @JsonSubTypes.Type(value = ContainsCondition.class, name = "contains"),
               @JsonSubTypes.Type(value = DateRangeCondition.class, name = "date_range"),
               @JsonSubTypes.Type(value = FuzzyCondition.class, name = "fuzzy"),
               @JsonSubTypes.Type(value = GeoDistanceCondition.class, name = "geo_distance"),
               @JsonSubTypes.Type(value = GeoBBoxCondition.class, name = "geo_bbox"),
               @JsonSubTypes.Type(value = GeoShapeCondition.class, name = "geo_shape"),
               @JsonSubTypes.Type(value = LuceneCondition.class, name = "lucene"),
               @JsonSubTypes.Type(value = MatchCondition.class, name = "match"),
               @JsonSubTypes.Type(value = NoneCondition.class, name = "none"),
               @JsonSubTypes.Type(value = PhraseCondition.class, name = "phrase"),
               @JsonSubTypes.Type(value = PrefixCondition.class, name = "prefix"),
               @JsonSubTypes.Type(value = RangeCondition.class, name = "range"),
               @JsonSubTypes.Type(value = RegexpCondition.class, name = "regexp"),
               @JsonSubTypes.Type(value = WildcardCondition.class, name = "wildcard")})
public abstract class Condition<T extends Condition> extends JSONBuilder {

    /** The boost for the {@code Condition} to be built. */
    @JsonProperty("boost")
    Float boost;

    /**
     * Sets the boost for the {@code Condition} to be built. Documents matching this condition will (in addition to the
     * normal weightings) have their score multiplied by {@code boost}. Defaults to {@code 1.0}.
     *
     * @param boost the boost for the {@code Condition} to be built
     * @return this with the specified boost
     */
    @SuppressWarnings("unchecked")
    public T boost(float boost) {
        this.boost = boost;
        return (T) this;
    }

    /**
     * Sets the boost for the {@code Condition} to be built. Documents matching this condition will (in addition to the
     * normal weightings) have their score multiplied by {@code boost}. Defaults to {@code 1.0}.
     *
     * @param boost the boost for the {@code Condition} to be built
     * @return this with the specified boost
     */
    @SuppressWarnings("unchecked")
    public T boost(Number boost) {
        this.boost = boost == null ? null : boost.floatValue();
        return (T) this;
    }
}
