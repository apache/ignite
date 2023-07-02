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

import org.hawkore.ignite.lucene.schema.mapping.StringMapper;

import com.fasterxml.jackson.annotation.JsonProperty;
 
/**
 * {@link SingleColumnMapperBuilder} to build a new {@link StringMapper}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class StringMapperBuilder extends SingleColumnMapperBuilder<StringMapper, StringMapperBuilder> {

    @JsonProperty("case_sensitive")
    private Boolean caseSensitive;

    /**
     * Sets if the {@link StringMapper} to be built must be case sensitive.
     *
     * @param caseSensitive if the {@link StringMapper} to be built must be case sensitive
     * @return this
     */
    public StringMapperBuilder caseSensitive(Boolean caseSensitive) {
        this.caseSensitive = caseSensitive;
        return this;
    }

    /**
     * Returns the {@link StringMapper} represented by this {@link MapperBuilder}.
     *
     * @param field the name of the field to be built
     * @return the {@link StringMapper} represented by this
     */
    @Override
    public StringMapper build(String field) {
        return new StringMapper(field, column, validated, caseSensitive);
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((caseSensitive == null) ? 0 : caseSensitive.hashCode());
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
        StringMapperBuilder other = (StringMapperBuilder) obj;
        if (caseSensitive == null) {
            if (other.caseSensitive != null)
                return false;
        } else if (!caseSensitive.equals(other.caseSensitive))
            return false;
        return true;
    }
}
