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
package org.hawkore.ignite.lucene.builder.search;

import java.util.List;

import org.hawkore.ignite.lucene.builder.JSONBuilder;
import org.hawkore.ignite.lucene.builder.search.condition.Condition;
import org.hawkore.ignite.lucene.builder.search.sort.Sort;
import org.hawkore.ignite.lucene.builder.search.sort.SortField;
import org.hawkore.ignite.lucene.search.SearchBuilder;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Class representing an Lucene index search. It is formed by an optional querying {@link Condition} and an optional
 * filtering {@link Condition}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
@SuppressWarnings("unused")
public class Search extends JSONBuilder {

    /** The filtering conditions not participating in scoring. */
    @JsonProperty("filter")
    private List<Condition> filter;

    /** The querying conditions participating in scoring. */
    @JsonProperty("query")
    private List<Condition> query;

    /** The {@link Sort} for the query, maybe {@code null} meaning no filtering. */
    @JsonProperty("sort")
    private List<SortField> sort;

    /** The paging size. */
    @JsonProperty("limit")
    private Integer limit;

    /** the offset, rows to skip */
    @JsonProperty("offset")
    private Integer offset;
    
    /** If this search must force the refresh the index before reading it. */
    @JsonProperty("refresh")
    private Boolean refresh;

    /** Default constructor. */
    public Search() {
    }

    /**
     * Returns this with the specified filtering conditions not participating in scoring.
     *
     * @param conditions the filtering conditions to be added
     * @return this with the specified filtering conditions
     */
    public Search filter(Condition... conditions) {
        filter = add(filter, conditions);
        return this;
    }

    /**
     * Returns this with the specified querying conditions participating in scoring.
     *
     * @param conditions the mandatory conditions to be added
     * @return this with the specified mandatory conditions
     */
    public Search query(Condition... conditions) {
        query = add(query, conditions);
        return this;
    }

    /**
     * Sets the sorting fields.
     *
     * @param fields the sorting fields to be added
     * @return this with the specified sorting fields
     */
    public Search sort(SortField... fields) {
        sort = add(sort, fields);
        return this;
    }

    /**
     * Sets if the {@link Search} must refresh the Lucene's index searcher before using it. Refresh is a costly
     * operation so you should use it only when it is strictly required.
     *
     * @param refresh if the {@link Search} must refresh the index before reading it
     * @return this with the specified refresh
     */
    public Search refresh(Boolean refresh) {
        this.refresh = refresh;
        return this;
    }
    

    /**
     *  The paging size. 
     *  
     * @param limit
     * 
     * @return this for chaining
     */
    public Search limit(int limit) {
        this.limit = limit;
        return this;
    }
    
    /** 
     * the offset, rows to skip
     * 
     * @param offset
     * 
     * @return this for chaining
     */
    public Search offset(int offset) {
        this.offset = offset;
        return this;
    }

}
