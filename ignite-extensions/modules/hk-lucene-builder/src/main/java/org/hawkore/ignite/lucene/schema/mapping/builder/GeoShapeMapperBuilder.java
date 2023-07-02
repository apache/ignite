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

import java.util.Arrays;
import java.util.List;

import org.hawkore.ignite.lucene.common.GeoTransformation;
import org.hawkore.ignite.lucene.common.JTSNotFoundException;
import org.hawkore.ignite.lucene.schema.mapping.GeoPointMapper;
import org.hawkore.ignite.lucene.schema.mapping.GeoShapeMapper;

import com.fasterxml.jackson.annotation.JsonProperty;
 
/**
 * {@link MapperBuilder} to build a new {@link GeoPointMapper}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeoShapeMapperBuilder extends MapperBuilder<GeoShapeMapper, GeoShapeMapperBuilder> {

    /** The name of the column to be mapped. */
    @JsonProperty("column")
    private String column;

    /** The maximum number of levels in the tree. */
    @JsonProperty("max_levels")
    private Integer maxLevels;

    /** The sequence of transformations to be applied to the shape before indexing. */
    @JsonProperty("transformations")
    private List<GeoTransformation> transformations;

    /**
     * Sets the name of the QueryEntity column to be mapped.
     *
     * @param column The name of the QueryEntity column to be mapped.
     * @return This.
     */
    public final GeoShapeMapperBuilder column(String column) {
        this.column = column;
        return this;
    }

    /**
     * Sets the maximum number of levels in the tree.
     *
     * @param maxLevels the maximum number of levels in the tree
     * @return this
     */
    public GeoShapeMapperBuilder maxLevels(Integer maxLevels) {
        this.maxLevels = maxLevels;
        return this;
    }

    /**
     * Sets the transformations to be applied to the shape before using it for searching.
     *
     * @param transformations the sequence of transformations
     * @return this with the transformations set
     */
    public GeoShapeMapperBuilder transformations(List<GeoTransformation> transformations) {
        this.transformations = transformations;
        return this;
    }

    /**
     * Sets the transformations to be applied to the shape before using it for searching.
     *
     * @param transformations the sequence of transformations
     * @return this with the transformations set
     */
    public GeoShapeMapperBuilder transformations(GeoTransformation... transformations) {
        return transformations(Arrays.asList(transformations));
    }

    /**
     * Returns the {@link GeoShapeMapper} represented by this {@link MapperBuilder}.
     *
     * @param field the name of the field to be built
     * @return the {@link GeoShapeMapper} represented by this
     */
    @Override
    public GeoShapeMapper build(String field) {
        try {
            return new GeoShapeMapper(field, column, validated, maxLevels, transformations);
        } catch (NoClassDefFoundError e) {
            throw new JTSNotFoundException();
        }
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((column == null) ? 0 : column.hashCode());
        result = prime * result + ((maxLevels == null) ? 0 : maxLevels.hashCode());
        result = prime * result + ((transformations == null) ? 0 : transformations.hashCode());
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
        GeoShapeMapperBuilder other = (GeoShapeMapperBuilder) obj;
        if (column == null) {
            if (other.column != null)
                return false;
        } else if (!column.equals(other.column))
            return false;
        if (maxLevels == null) {
            if (other.maxLevels != null)
                return false;
        } else if (!maxLevels.equals(other.maxLevels))
            return false;
        if (transformations == null) {
            if (other.transformations != null)
                return false;
        } else if (!transformations.equals(other.transformations))
            return false;
        return true;
    }
    
    
}
