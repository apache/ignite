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

import org.hawkore.ignite.lucene.search.condition.BitemporalCondition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * {@link ConditionBuilder} for building a new {@link BitemporalCondition}.
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class BitemporalConditionBuilder extends ConditionBuilder<BitemporalCondition, BitemporalConditionBuilder> {

    /** The name of the filed to be matched. */
    @JsonProperty("field")
    private final String field;

    /** The valid time start. */
    @JsonProperty("vt_from")
    private Object vtFrom;

    /** The valid time end. */
    @JsonProperty("vt_to")
    private Object vtTo;

    /** The transaction time start. */
    @JsonProperty("tt_from")
    private Object ttFrom;

    /** The transaction time end. */
    @JsonProperty("tt_to")
    private Object ttTo;

    /**
     * Returns a new {@link BitemporalConditionBuilder} with the specified field reference point.
     *
     * @param field the name of the field to be matched
     */
    @JsonCreator
    public BitemporalConditionBuilder(@JsonProperty("field") String field) {
        this.field = field;
    }

    /**
     * Sets the valid time start.
     *
     * @param vtFrom the valid time start to be set
     * @return this with the specified valid time start
     */
    public BitemporalConditionBuilder vtFrom(Object vtFrom) {
        this.vtFrom = vtFrom;
        return this;
    }

    /**
     * Sets the valid time end.
     *
     * @param vtTo the valid time end to be set
     * @return this with the specified valid time end
     */
    public BitemporalConditionBuilder vtTo(Object vtTo) {
        this.vtTo = vtTo;
        return this;
    }

    /**
     * Sets the transaction time start.
     *
     * @param ttFrom the transaction time start to be set
     * @return this with the specified transaction time start
     */
    public BitemporalConditionBuilder ttFrom(Object ttFrom) {
        this.ttFrom = ttFrom;
        return this;
    }

    /**
     * Sets the transaction time end.
     *
     * @param ttTo the transaction time end to be set
     * @return this with the specified transaction time end
     */
    public BitemporalConditionBuilder ttTo(Object ttTo) {
        this.ttTo = ttTo;
        return this;
    }

    /**
     * Returns the {@link BitemporalCondition} represented by this builder.
     *
     * @return a new bitemporal condition
     */
    @Override
    public BitemporalCondition build() {
        return new BitemporalCondition(boost, field, vtFrom, vtTo, ttFrom, ttTo);
    }
}
