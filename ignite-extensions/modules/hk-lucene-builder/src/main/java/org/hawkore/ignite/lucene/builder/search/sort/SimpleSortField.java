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
package org.hawkore.ignite.lucene.builder.search.sort;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A simple relevance sorting for a field of a search.
 *
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class SimpleSortField extends SortField {

    /** The name of the field to be used for sorting. */
    @JsonProperty("field")
    final String field;

    /**
     * Creates a new {@link SimpleSortField} for the specified field and reverse option.
     *
     * @param field the name of the field to be used for sorting
     */
    @JsonCreator
    public SimpleSortField(@JsonProperty("field") String field) {
        this.field = field;
    }
}