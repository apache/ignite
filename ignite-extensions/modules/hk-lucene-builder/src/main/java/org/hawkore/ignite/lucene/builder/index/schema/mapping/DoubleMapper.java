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
package org.hawkore.ignite.lucene.builder.index.schema.mapping;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A {@link Mapper} to map a double field.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class DoubleMapper extends SingleColumnMapper<DoubleMapper> {

    /** The field's index-time boost. */
    @JsonProperty("boost")
    Float boost;

    /**
     * Sets the boost to be used.
     *
     * @param boost the boost
     * @return this with the specified boost
     */
    public DoubleMapper boost(float boost) {
        this.boost = boost;
        return this;
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
        DoubleMapper other = (DoubleMapper) obj;
        if (boost == null) {
            if (other.boost != null)
                return false;
        } else if (!boost.equals(other.boost))
            return false;
        return true;
    }
    
}
