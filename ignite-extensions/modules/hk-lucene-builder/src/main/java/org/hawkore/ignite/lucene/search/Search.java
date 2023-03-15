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
package org.hawkore.ignite.lucene.search;

import static org.apache.lucene.search.BooleanClause.Occur.FILTER;
import static org.apache.lucene.search.BooleanClause.Occur.MUST;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.hawkore.ignite.lucene.schema.Schema;
import org.hawkore.ignite.lucene.search.condition.Condition;
import org.hawkore.ignite.lucene.search.sort.SortField;

import com.google.common.base.MoreObjects;

/**
 * Class representing an Lucene index search. It can be translated to a Lucene {@link Query} using a {@link Schema}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class Search {

    private static final boolean DEFAULT_FORCE_REFRESH = false;

    /** The mandatory conditions not participating in scoring. */
    public final List<Condition> filter;

    /** The mandatory conditions participating in scoring. */
    public final List<Condition> query;

    /** The sorting fields for the query. */
    private final List<SortField> sort;

    /** If this search must refresh the index before reading it. */
    private final Boolean refresh;

    /** The paging size. */
    private final Integer limit;

    /** the offset, rows to skip */
    private final Integer offset;
    
    
    /**
     * Constructor using the specified querying, filtering, sorting and refresh options.
     *
     * @param filter the filtering {@link Condition}s not involved in scoring
     * @param query the querying {@link Condition}s participating in scoring
     * @param sort the sort fields for the query
     * @param limit the page size
     * @param offset the rows to skip
     * @param refresh if this search must refresh the index before reading it
     */
    public Search(List<Condition> filter,
                  List<Condition> query,
                  List<SortField> sort,
                  Integer limit,
                  Integer offset,
                  Boolean refresh) {
        this.filter = filter == null ? Collections.EMPTY_LIST : filter;
        this.query = query == null ? Collections.EMPTY_LIST : query;
        this.sort = sort == null ? Collections.EMPTY_LIST : sort;
        this.limit = limit;
        this.offset = offset;
        this.refresh = refresh == null ? DEFAULT_FORCE_REFRESH : refresh;
    }

    /**
     * Returns if this search requires post reconciliation agreement processing to preserve the order of its results.
     *
     * @return {@code true} if it requires post processing, {@code false} otherwise
     */
    public boolean requiresPostProcessing() {
        return usesRelevance() || usesSorting();
    }

    /**
     * Returns if this search requires full ranges scan.
     *
     * @return {@code true} if this search requires full ranges scan, {code null} otherwise
     */
    public boolean requiresFullScan() {
        return usesRelevance() || usesSorting() || refresh && isEmpty();
    }

    /**
     * Returns if this search uses Lucene relevance formula.
     *
     * @return {@code true} if this search uses Lucene relevance formula, {@code false} otherwise
     */
    public boolean usesRelevance() {
        return !query.isEmpty();
    }

    /**
     * Returns if this search uses field sorting.
     *
     * @return {@code true} if this search uses field sorting, {@code false} otherwise
     */
    public boolean usesSorting() {
        return !sort.isEmpty();
    }

    /**
     * Returns if this search doesn't specify any filter, query or sort.
     *
     * @return {@code true} if this search doesn't specify any filter, query or sort, {@code false} otherwise
     */
    public boolean isEmpty() {
        return filter.isEmpty() && query.isEmpty() && sort.isEmpty();
    }

    /**
     * Returns the Lucene {@link Query} represented by this search, with the additional optional data range filter.
     *
     * @param schema the indexing schema
     * @param range the additional data range filter, maybe {@code null}
     * @return a Lucene {@link Query}
     */
    public Query query(Schema schema, Query... filters) {

        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        if (filters != null && filters.length>0) {
    		IntStream.range(0, filters.length).forEach(i -> {if(filters[i]!=null)builder.add(filters[i], FILTER);});
        }

        filter.forEach(condition -> builder.add(condition.query(schema), FILTER));
        query.forEach(condition -> builder.add(condition.query(schema), MUST));

        BooleanQuery booleanQuery = builder.build();
        return booleanQuery.clauses().isEmpty() ? new MatchAllDocsQuery() : booleanQuery;
    }
    

    public Query postProcessingQuery(Schema schema) {
        if (query.isEmpty()) {
            return new MatchAllDocsQuery();
        } else {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            query.forEach(condition -> builder.add(condition.query(schema), MUST));
            return builder.build();
        }
    }

    /**
     * Returns if this search needs to refresh the index before reading it.
     *
     * @return {@code true} if this search needs to refresh the index before reading it, {@code false} otherwise.
     */
    public boolean refresh() {
        return refresh;
    }

    /**
     * Returns the Lucene {@link org.apache.lucene.search.SortField}s represented by this using the specified schema.
     *
     * @param schema the indexing schema to be used
     * @return the Lucene sort fields represented by this using {@code schema}
     */
    public List<org.apache.lucene.search.SortField> sortFields(Schema schema) {
        return sort.stream().map(s -> s.sortField(schema)).collect(Collectors.toList());
    }

    
    /**
     * Returns the names of the involved fields when post processing.
     *
     * @return the names of the involved fields
     */
    public Set<String> postProcessingFields() {
        Set<String> fields = new LinkedHashSet<>();
        query.forEach(condition -> fields.addAll(condition.postProcessingFields()));
        sort.forEach(condition -> fields.addAll(condition.postProcessingFields()));
        return fields;
    }

    /**
     * @return the limit
     */
    public Integer limit() {
        return limit;
    }

    /**
     * @return the offset
     */
    public Integer offset() {
        return offset;
    }

    /**
     * Validates this {@link Search} against the specified {@link Schema}.
     *
     * @param schema a {@link Schema}
     * @return this
     */
    public Search validate(Schema schema) {
        filter.forEach(condition -> condition.query(schema));
        query.forEach(condition -> condition.query(schema));
        sort.forEach(field -> field.sortField(schema));
        return this;
    }

    
    /**
     * @return the sort
     */
    public List<SortField> sort() {
        return sort;
    }
    
    /** {@inheritDoc} */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("filter", filter)
                          .add("query", query)
                          .add("sort", sort)
                          .add("refresh", refresh)
                          .add("limit", limit)
                          .add("offset", offset)
                          .toString();
    }
}
