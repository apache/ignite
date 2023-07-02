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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A {@link Condition} implementation that matches bi-temporal (four) fields within two ranges of values.
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class BitemporalCondition extends Condition<BitemporalCondition> {

    /** The name of the filed to be matched. */
    @JsonProperty("field")
    final String field;

    /** The valid time start. */
    @JsonProperty("vt_from")
    Object vtFrom;

    /** The valid time end. */
    @JsonProperty("vt_to")
    Object vtTo;

    /** The transaction time start. */
    @JsonProperty("tt_from")
    Object ttFrom;

    /** The transaction time end. */
    @JsonProperty("tt_to")
    Object ttTo;

    /**
     * Returns a new {@link BitemporalCondition} for the specified field.
     *
     * @param field the name of the field
     */
    @JsonCreator
    public BitemporalCondition(@JsonProperty("field") String field) {
        this.field = field;
    }

    /**
     * Sets the valid time start.
     *
     * @param vtFrom the valid time start
     * @return this with the specified valid time start
     */
    public BitemporalCondition vtFrom(Object vtFrom) {
        this.vtFrom = vtFrom;
        return this;
    }

    /**
     * Sets the valid time end.
     *
     * @param vtTo the valid time end
     * @return this with the specified valid time end
     */
    public BitemporalCondition vtTo(Object vtTo) {
        this.vtTo = vtTo;
        return this;
    }

    /**
     * Sets the transaction time start.
     *
     * @param ttFrom the transaction time start
     * @return this with the specified transaction time start
     */
    public BitemporalCondition ttFrom(Object ttFrom) {
        this.ttFrom = ttFrom;
        return this;
    }

    /**
     * Sets the transaction time end.
     *
     * @param ttTo the transaction time end
     * @return this with the specified transaction time end
     */
    public BitemporalCondition ttTo(Object ttTo) {
        this.ttTo = ttTo;
        return this;
    }
}
