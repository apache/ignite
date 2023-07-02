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

import org.hawkore.ignite.lucene.builder.common.GeoShape;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * {@link Condition} for building a new {@link GeoShapeCondition}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class GeoShapeCondition extends Condition<GeoShapeCondition> {

    /** The name of the field to be matched. */
    @JsonProperty("field")
    final String field;

    /** The shape. */
    @JsonProperty("shape")
    final GeoShape shape;

    /** The spatial operation to be applied. */
    @JsonProperty("operation")
    String operation;

    /**
     * Constructor receiving the name of the field and the shape.
     *
     * @param field the name of the field
     * @param shape the shape
     */
    @JsonCreator
    public GeoShapeCondition(@JsonProperty("field") String field, @JsonProperty("shape") GeoShape shape) {
        this.field = field;
        this.shape = shape;
    }

    /**
     * Sets the name of the spatial operation to be performed.
     *
     * @param operation the name of the spatial operation
     * @return this with the operation set
     */
    public GeoShapeCondition operation(String operation) {
        this.operation = operation;
        return this;
    }
}