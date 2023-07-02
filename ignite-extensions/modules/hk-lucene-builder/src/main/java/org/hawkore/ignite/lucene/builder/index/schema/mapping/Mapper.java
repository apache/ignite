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

import org.hawkore.ignite.lucene.builder.JSONBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Class for mapping between QueryEntity's columns and Lucene documents.
 *
 * @param <T> the type of the mapper to be built
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({@JsonSubTypes.Type(value = BlobMapper.class, name = "bytes"),
               @JsonSubTypes.Type(value = BooleanMapper.class, name = "boolean"),
               @JsonSubTypes.Type(value = DateMapper.class, name = "date"),
               @JsonSubTypes.Type(value = DoubleMapper.class, name = "double"),
               @JsonSubTypes.Type(value = FloatMapper.class, name = "float"),
               @JsonSubTypes.Type(value = InetMapper.class, name = "inet"),
               @JsonSubTypes.Type(value = IntegerMapper.class, name = "integer"),
               @JsonSubTypes.Type(value = LongMapper.class, name = "long"),
               @JsonSubTypes.Type(value = StringMapper.class, name = "string"),
               @JsonSubTypes.Type(value = TextMapper.class, name = "text"),
               @JsonSubTypes.Type(value = UUIDMapper.class, name = "uuid"),
               @JsonSubTypes.Type(value = BigDecimalMapper.class, name = "bigdec"),
               @JsonSubTypes.Type(value = BigIntegerMapper.class, name = "bigint"),
               @JsonSubTypes.Type(value = GeoPointMapper.class, name = "geo_point"),
               @JsonSubTypes.Type(value = GeoShapeMapper.class, name = "geo_shape"),
               @JsonSubTypes.Type(value = DateRangeMapper.class, name = "date_range"),
               @JsonSubTypes.Type(value = BitemporalMapper.class, name = "bitemporal")})
public abstract class Mapper<T extends Mapper> extends JSONBuilder {

    /** If the field must be validated. */
    @JsonProperty("validated")
    protected Boolean validated;

    /**
     * Sets if the field must be validated.
     *
     * @param validated if the field must be validated
     * @return this with the specified {@code validated} option
     */
    @SuppressWarnings("unchecked")
    public final T validated(Boolean validated) {
        this.validated = validated;
        return (T) this;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((validated == null) ? 0 : validated.hashCode());
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Mapper other = (Mapper) obj;
        if (validated == null) {
            if (other.validated != null)
                return false;
        } else if (!validated.equals(other.validated))
            return false;
        return true;
    }
    
    
}
