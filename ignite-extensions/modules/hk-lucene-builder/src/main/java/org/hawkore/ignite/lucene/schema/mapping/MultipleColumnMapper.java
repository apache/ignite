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
package org.hawkore.ignite.lucene.schema.mapping;

import java.util.List;

/**
 * Class for mapping between QueryEntity's columns and Lucene documents.
 *
 * @param <T> The base type.
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public abstract class MultipleColumnMapper<T extends Comparable<T>> extends Mapper {

    /**
     * Builds a new {@link SingleColumnMapper} supporting the specified types for indexing and clustering.
     *
     * @param field the name of the field
     * @param validated if the field must be validated
     * @param mappedColumns the names of the columns to be mapped
     * @param supportedTypes the supported column value data types
     */
    public MultipleColumnMapper(String field,
                                Boolean validated,
                                List<String> mappedColumns,
                                List<Class<?>> supportedTypes) {
        super(field,
              false,
              validated,
              null,
              mappedColumns,
              supportedTypes,
              EMPTY_TYPE_LIST,
              false);
    }

    /**
     * Builds a new {@link SingleColumnMapper} supporting the specified types for indexing and clustering.
     *
     * @param field the name of the field
     * @param validated if the field must be validated
     * @param mappedColumns the names of the columns to be mapped
     * @param supportedTypes the supported column value data types
     * @param excludedTypes the explicitly excluded value data types
     */
    public MultipleColumnMapper(String field,
                                Boolean validated,
                                List<String> mappedColumns,
                                List<Class<?>> supportedTypes,
                                List<Class<?>> excludedTypes ) {
        super(field,
              false,
              validated,
              null,
              mappedColumns,
              supportedTypes,
              excludedTypes,
              false);
    }

    /** {@inheritDoc} */
    @Override
    public abstract String toString();

}
