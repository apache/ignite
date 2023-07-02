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
package org.hawkore.ignite.lucene.common;

import org.apache.lucene.spatial.query.SpatialOperation;
import org.hawkore.ignite.lucene.IndexException;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Enum representing a spatial operation.
 */
public enum GeoOperation {

    INTERSECTS("intersects", SpatialOperation.Intersects),
    IS_WITHIN("is_within", SpatialOperation.IsWithin),
    CONTAINS("contains", SpatialOperation.Contains);

    private final String name;
    private final SpatialOperation spatialOperation;

    GeoOperation(String name, SpatialOperation spatialOperation) {
        this.name = name;
        this.spatialOperation = spatialOperation;
    }

    /**
     * Returns the represented Lucene's {@link SpatialOperation}.
     *
     * @return the Lucene's spatial operation
     */
    public SpatialOperation getSpatialOperation() {
        return spatialOperation;
    }

    @JsonCreator
    public static GeoOperation parse(String value) {
        for (GeoOperation geoOperation : values()) {
            String name = geoOperation.name;
            if (name.equalsIgnoreCase(value)) {
                return geoOperation;
            }
        }
        throw new IndexException("Invalid geographic operation {}", value);
    }
}
