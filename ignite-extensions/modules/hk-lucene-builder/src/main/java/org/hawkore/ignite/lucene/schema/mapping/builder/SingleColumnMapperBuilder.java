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

import org.hawkore.ignite.lucene.schema.mapping.SingleColumnMapper;

import com.fasterxml.jackson.annotation.JsonProperty;
 
/**
 * Abstract {@link MapperBuilder} for creating new {@link SingleColumnMapper}s.
 *
 * @param <T> The {@link SingleColumnMapper} to be built.
 * @param <K> The specific {@link SingleColumnMapper}.
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public abstract class SingleColumnMapperBuilder<T extends SingleColumnMapper<?>, K extends SingleColumnMapperBuilder<T, K>>
        extends MapperBuilder<T, K> {

    /** The name of the column to be mapped. */
    @JsonProperty("column")
    protected String column;

    /**
     * Sets the name of the QueryEntity column to be mapped.
     *
     * @param column the name of the QueryEntity column to be mapped
     * @return this
     */
    @SuppressWarnings("unchecked")
    public final K column(String column) {
        this.column = column;
        return (K) this;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((column == null) ? 0 : column.hashCode());
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
        SingleColumnMapperBuilder other = (SingleColumnMapperBuilder) obj;
        if (column == null) {
            if (other.column != null)
                return false;
        } else if (!column.equals(other.column))
            return false;
        return true;
    }
    
    
}
