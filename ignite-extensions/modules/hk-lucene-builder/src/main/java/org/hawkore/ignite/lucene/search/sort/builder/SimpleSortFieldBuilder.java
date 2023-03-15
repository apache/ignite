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
package org.hawkore.ignite.lucene.search.sort.builder;

import org.hawkore.ignite.lucene.search.sort.SimpleSortField;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class SimpleSortFieldBuilder extends SortFieldBuilder<SimpleSortField, SimpleSortFieldBuilder> {

    /** The name of the field to be used for sort. */
    @JsonProperty("field")
    final String field;

    /**
     * Creates a new {@link SimpleSortFieldBuilder} for the specified field.
     *
     * @param field The field to sort by.
     */
    @JsonCreator
    public SimpleSortFieldBuilder(@JsonProperty("field") String field) {
        this.field = field;
    }

    /** {@inheritDoc} */
    @Override
    public SimpleSortField build() {
        return new SimpleSortField(field, reverse);
    }
}