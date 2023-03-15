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

import org.hawkore.ignite.lucene.schema.mapping.DoubleMapper;

import com.fasterxml.jackson.annotation.JsonProperty;
 
/**
 * {@link SingleColumnMapperBuilder} to build a new {@link DoubleMapper}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class DoubleMapperBuilder extends SingleColumnMapperBuilder<DoubleMapper, DoubleMapperBuilder> {

    @JsonProperty("boost")
    private Float boost;

    /**
     * Sets the boost to be used.
     *
     * @param boost the boost to be used
     * @return this
     */
    public DoubleMapperBuilder boost(Float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Returns the {@link DoubleMapper} represented by this {@link MapperBuilder}.
     *
     * @param field the name of the field to be built
     * @return the {@link DoubleMapper} represented by this
     */
    @Override
    public DoubleMapper build(String field) {
        return new DoubleMapper(field, column, validated, boost);
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((boost == null) ? 0 : boost.hashCode());
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
        DoubleMapperBuilder other = (DoubleMapperBuilder) obj;
        if (boost == null) {
            if (other.boost != null)
                return false;
        } else if (!boost.equals(other.boost))
            return false;
        return true;
    }
}
