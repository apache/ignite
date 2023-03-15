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
package org.hawkore.ignite.lucene.schema.mapping.builder;

import org.hawkore.ignite.lucene.schema.mapping.BitemporalMapper;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * {@link MapperBuilder} to build a new {@link BitemporalMapperBuilder}.
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class BitemporalMapperBuilder extends MapperBuilder<BitemporalMapper, BitemporalMapperBuilder> {

    /** The name of the column containing the valid time start. **/
    @JsonProperty("vt_from")
    private final String vtFrom;

    /** The name of the column containing the valid time stop. **/
    @JsonProperty("vt_to")
    private final String vtTo;

    /** The name of the column containing the transaction time start. **/
    @JsonProperty("tt_from")
    private final String ttFrom;

    /** The name of the column containing the transaction time stop. **/
    @JsonProperty("tt_to")
    private final String ttTo;

    /** The date pattern */
    @JsonProperty("pattern")
    private String pattern;

    /** The NOW Value. **/
    @JsonProperty("now_value")
    private Object nowValue;

    /**
     * Returns a new {@link BitemporalMapperBuilder}.
     *
     * @param vtFrom the column name containing the valid time start
     * @param vtTo the column name containing the valid time stop
     * @param ttFrom the column name containing the transaction time start
     * @param ttTo the column name containing the transaction time stop
     */
    @JsonCreator
    public BitemporalMapperBuilder(@JsonProperty("vt_from") String vtFrom,
                                   @JsonProperty("vt_to") String vtTo,
                                   @JsonProperty("tt_from") String ttFrom,
                                   @JsonProperty("tt_to") String ttTo) {
        this.vtFrom = vtFrom;
        this.vtTo = vtTo;
        this.ttFrom = ttFrom;
        this.ttTo = ttTo;
    }

    /**
     * Sets the date format pattern to be used.
     *
     * @param pattern a {@link java.text.SimpleDateFormat} date pattern
     * @return this
     */
    public BitemporalMapperBuilder pattern(String pattern) {
        this.pattern = pattern;
        return this;
    }

    /**
     * Sets the now value to be used.
     *
     * @param nowValue the now value to be used
     * @return this
     */
    public BitemporalMapperBuilder nowValue(Object nowValue) {
        this.nowValue = nowValue;
        return this;
    }

    /**
     * Returns the {@link BitemporalMapper} represented by this {@link MapperBuilder}.
     *
     * @param field the name of the field to be built
     * @return the {@link BitemporalMapper} represented by this
     */
    @Override
    public BitemporalMapper build(String field) {
        return new BitemporalMapper(field, validated, vtFrom, vtTo, ttFrom, ttTo, pattern, nowValue);
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((nowValue == null) ? 0 : nowValue.hashCode());
        result = prime * result + ((pattern == null) ? 0 : pattern.hashCode());
        result = prime * result + ((ttFrom == null) ? 0 : ttFrom.hashCode());
        result = prime * result + ((ttTo == null) ? 0 : ttTo.hashCode());
        result = prime * result + ((vtFrom == null) ? 0 : vtFrom.hashCode());
        result = prime * result + ((vtTo == null) ? 0 : vtTo.hashCode());
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        BitemporalMapperBuilder other = (BitemporalMapperBuilder) obj;
        if (nowValue == null) {
            if (other.nowValue != null)
                return false;
        } else if (!nowValue.equals(other.nowValue))
            return false;
        if (pattern == null) {
            if (other.pattern != null)
                return false;
        } else if (!pattern.equals(other.pattern))
            return false;
        if (ttFrom == null) {
            if (other.ttFrom != null)
                return false;
        } else if (!ttFrom.equals(other.ttFrom))
            return false;
        if (ttTo == null) {
            if (other.ttTo != null)
                return false;
        } else if (!ttTo.equals(other.ttTo))
            return false;
        if (vtFrom == null) {
            if (other.vtFrom != null)
                return false;
        } else if (!vtFrom.equals(other.vtFrom))
            return false;
        if (vtTo == null) {
            if (other.vtTo != null)
                return false;
        } else if (!vtTo.equals(other.vtTo))
            return false;
        return true;
    }
}
