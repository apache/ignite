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

import java.util.Arrays;
import java.util.List;

import org.hawkore.ignite.lucene.builder.Builder;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A sorting of fields for a search.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class Sort extends Builder {

    /** The {@link SortField}s. */
    @JsonProperty("fields")
    final List<SortField> sortFields;

    /**
     * Creates a new {@link Sort} for the specified {@link SortField}.
     *
     * @param sortFields the {@link SortField}s
     */
    public Sort(List<SortField> sortFields) {
        this.sortFields = sortFields;
    }

    /**
     * Creates a new {@link Sort} for the specified {@link SortField}.
     *
     * @param sortFields the {@link SortField}s
     */
    @JsonCreator
    public Sort(@JsonProperty("fields") SortField... sortFields) {
        this(Arrays.asList(sortFields));
    }
}
