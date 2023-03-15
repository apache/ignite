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

import org.hawkore.ignite.lucene.schema.mapping.DateRangeMapper;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * {@link MapperBuilder} to build a new {@link DateRangeMapper}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class DateRangeMapperBuilder extends MapperBuilder<DateRangeMapper, DateRangeMapperBuilder> {

    /** The column containing the start date. */
    @JsonProperty("from")
    private final String from;

    /** The column containing the stop date. */
    @JsonProperty("to")
    private final String to;

    /** The date pattern */
    @JsonProperty("pattern")
    private String pattern;

    /**
     * Returns a new {@link DateRangeMapperBuilder}.
     *
     * @param from the column containing the start date
     * @param to the column containing the stop date
     */
    public DateRangeMapperBuilder(@JsonProperty("from") String from, @JsonProperty("to") String to) {
        this.from = from;
        this.to = to;
    }

    /**
     * Sets the date pattern to be used both for columns and fields.
     *
     * @param pattern a {@link java.text.SimpleDateFormat} date pattern
     * @return this
     */
    public DateRangeMapperBuilder pattern(String pattern) {
        this.pattern = pattern;
        return this;
    }

    /**
     * Returns the {@link DateRangeMapper} represented by this {@link MapperBuilder}.
     *
     * @param field the name of the field to be built
     * @return the {@link DateRangeMapper} represented by this
     */
    @Override
    public DateRangeMapper build(String field) {
        return new DateRangeMapper(field, validated, from, to, pattern);
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((from == null) ? 0 : from.hashCode());
        result = prime * result + ((pattern == null) ? 0 : pattern.hashCode());
        result = prime * result + ((to == null) ? 0 : to.hashCode());
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
        DateRangeMapperBuilder other = (DateRangeMapperBuilder) obj;
        if (from == null) {
            if (other.from != null)
                return false;
        } else if (!from.equals(other.from))
            return false;
        if (pattern == null) {
            if (other.pattern != null)
                return false;
        } else if (!pattern.equals(other.pattern))
            return false;
        if (to == null) {
            if (other.to != null)
                return false;
        } else if (!to.equals(other.to))
            return false;
        return true;
    }
    
    
}
