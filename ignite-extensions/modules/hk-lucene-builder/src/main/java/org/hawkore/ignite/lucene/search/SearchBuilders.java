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

import org.hawkore.ignite.lucene.common.GeoShape;
import org.hawkore.ignite.lucene.search.condition.builder.AllConditionBuilder;
import org.hawkore.ignite.lucene.search.condition.builder.BitemporalConditionBuilder;
import org.hawkore.ignite.lucene.search.condition.builder.BooleanConditionBuilder;
import org.hawkore.ignite.lucene.search.condition.builder.ConditionBuilder;
import org.hawkore.ignite.lucene.search.condition.builder.ContainsConditionBuilder;
import org.hawkore.ignite.lucene.search.condition.builder.DateRangeConditionBuilder;
import org.hawkore.ignite.lucene.search.condition.builder.FuzzyConditionBuilder;
import org.hawkore.ignite.lucene.search.condition.builder.GeoBBoxConditionBuilder;
import org.hawkore.ignite.lucene.search.condition.builder.GeoDistanceConditionBuilder;
import org.hawkore.ignite.lucene.search.condition.builder.GeoShapeConditionBuilder;
import org.hawkore.ignite.lucene.search.condition.builder.LuceneConditionBuilder;
import org.hawkore.ignite.lucene.search.condition.builder.MatchConditionBuilder;
import org.hawkore.ignite.lucene.search.condition.builder.NoneConditionBuilder;
import org.hawkore.ignite.lucene.search.condition.builder.PhraseConditionBuilder;
import org.hawkore.ignite.lucene.search.condition.builder.PrefixConditionBuilder;
import org.hawkore.ignite.lucene.search.condition.builder.RangeConditionBuilder;
import org.hawkore.ignite.lucene.search.condition.builder.RegexpConditionBuilder;
import org.hawkore.ignite.lucene.search.condition.builder.WildcardConditionBuilder;
import org.hawkore.ignite.lucene.search.sort.builder.GeoDistanceSortFieldBuilder;
import org.hawkore.ignite.lucene.search.sort.builder.SimpleSortFieldBuilder;
import org.hawkore.ignite.lucene.search.sort.builder.SortFieldBuilder;

/**
 * Factory for {@link SearchBuilder} and {@link ConditionBuilder}s.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public final class SearchBuilders {

    /** Private constructor to hide the implicit public one. */
    private SearchBuilders() {
    }

    /**
     * Returns a new {@link SearchBuilder}.
     *
     * @return the search builder
     */
    public static SearchBuilder search() {
        return new SearchBuilder();
    }

    /**
     * Returns a new {@link SearchBuilder} using the specified filtering {@link ConditionBuilder}s to not be used in
     * scoring.
     *
     * @param filters the condition builders to be used as filter
     * @return a new {@link SearchBuilder}
     */
    public static SearchBuilder filter(ConditionBuilder<?, ?>... filters) {
        return search().filter(filters);
    }

    /**
     * Returns a new {@link SearchBuilder} using the specified querying {@link ConditionBuilder}s o be used in scoring.
     *
     * @param queries the condition builders to be used as query
     * @return a new {@link SearchBuilder}
     */
    public static SearchBuilder query(ConditionBuilder<?, ?>... queries) {
        return search().query(queries);
    }

    /**
     * Returns a new {@link SearchBuilder} using the specified {@link SortFieldBuilder}s as sorting.
     *
     * @param sortFields the sorting builder
     * @return a new {@link SearchBuilder} with the specified sort
     */
    public static SearchBuilder sort(SortFieldBuilder... sortFields) {
        return search().sort(sortFields);
    }

    /**
     * Returns a new {@link SearchBuilder} using the specified index refresh option.
     *
     * @param refresh if the search to be built should refresh the index
     * @return a new {@link SearchBuilder} with the specified sort
     */
    public static SearchBuilder refresh(boolean refresh) {
        return search().refresh(refresh);
    }

    /**
     * Returns a new {@link BooleanConditionBuilder}.
     *
     * @return a new boolean condition builder
     */
    public static BooleanConditionBuilder bool() {
        return new BooleanConditionBuilder();
    }

    /**
     * Returns a new {@link AllConditionBuilder} for the specified field and value.
     *
     * @return a new all condition builder
     */
    public static AllConditionBuilder all() {
        return new AllConditionBuilder();
    }

    /**
     * Returns a new {@link FuzzyConditionBuilder} for the specified field and value.
     *
     * @param field the name of the field to be matched
     * @param value the value of the field to be matched
     * @return a new fuzzy condition builder
     */
    public static FuzzyConditionBuilder fuzzy(String field, String value) {
        return new FuzzyConditionBuilder(field, value);
    }

    /**
     * Returns a new {@link LuceneConditionBuilder} with the specified query.
     *
     * @param query the Lucene syntax query
     * @return a new Lucene condition builder
     */
    public static LuceneConditionBuilder lucene(String query) {
        return new LuceneConditionBuilder(query);
    }

    /**
     * Returns a new {@link MatchConditionBuilder} for the specified field and value.
     *
     * @param field the name of the field to be matched
     * @param value the value of the field to be matched
     * @return a new match condition builder
     */
    public static MatchConditionBuilder match(String field, Object value) {
        return new MatchConditionBuilder(field, value);
    }

    /**
     * Returns a new {@link ContainsConditionBuilder} for the specified field and value.
     *
     * @param field the name of the field to be matched
     * @param values the values of the field to be matched
     * @return a new match condition builder
     */
    public static ContainsConditionBuilder contains(String field, Object... values) {
        return new ContainsConditionBuilder(field, values);
    }

    /**
     * Returns a new {@link NoneConditionBuilder} for the specified field and value.
     *
     * @return a new none condition builder
     */
    public static NoneConditionBuilder none() {
        return new NoneConditionBuilder();
    }

    /**
     * Returns a new {@link PhraseConditionBuilder} for the specified field and values.
     *
     * @param field the name of the field to be matched
     * @param value The text to be matched.
     * @return a new phrase condition builder
     */
    public static PhraseConditionBuilder phrase(String field, String value) {
        return new PhraseConditionBuilder(field, value);
    }

    /**
     * Returns a new {@link PrefixConditionBuilder} for the specified field and value.
     *
     * @param field the name of the field to be matched
     * @param value the value of the field to be matched
     * @return a new prefix condition builder
     */
    public static PrefixConditionBuilder prefix(String field, String value) {
        return new PrefixConditionBuilder(field, value);
    }

    /**
     * Returns a new {@link RangeConditionBuilder} for the specified field.
     *
     * @param field the name of the field to be matched
     * @return a new range condition builder
     */
    public static RangeConditionBuilder range(String field) {
        return new RangeConditionBuilder(field);
    }

    /**
     * Returns a new {@link RegexpConditionBuilder} for the specified field and value.
     *
     * @param field the name of the field to be matched
     * @param value the value of the field to be matched
     * @return a new regexp condition builder
     */
    public static RegexpConditionBuilder regexp(String field, String value) {
        return new RegexpConditionBuilder(field, value);
    }

    /**
     * Returns a new {@link WildcardConditionBuilder} for the specified field and value.
     *
     * @param field the name of the field to be matched
     * @param value the value of the field to be matched
     * @return a new wildcard condition builder
     */
    public static WildcardConditionBuilder wildcard(String field, String value) {
        return new WildcardConditionBuilder(field, value);
    }

    /**
     * Returns a new {@link GeoBBoxConditionBuilder} with the specified field name and bounding box coordinates.
     *
     * @param field the name of the field to be matched
     * @param minLongitude The minimum accepted longitude.
     * @param maxLongitude The maximum accepted longitude.
     * @param minLatitude The minimum accepted latitude.
     * @param maxLatitude The maximum accepted latitude.
     * @return a new geo bounding box condition builder
     */
    public static GeoBBoxConditionBuilder geoBBox(String field,
                                                  double minLongitude,
                                                  double maxLongitude,
                                                  double minLatitude,
                                                  double maxLatitude) {
        return new GeoBBoxConditionBuilder(field, minLatitude, maxLatitude, minLongitude, maxLongitude);
    }

    /**
     * Returns a new {@link GeoDistanceConditionBuilder} with the specified field reference point.
     *
     * @param field the name of the field to be matched
     * @param latitude The latitude of the reference point.
     * @param longitude The longitude of the reference point.
     * @param maxDistance The max allowed distance.
     * @return a new geo distance condition builder
     */
    public static GeoDistanceConditionBuilder geoDistance(String field,
                                                          double latitude,
                                                          double longitude,
                                                          String maxDistance) {
        return new GeoDistanceConditionBuilder(field, latitude, longitude, maxDistance);
    }

    /**
     * Returns a new {@link GeoShapeConditionBuilder} with the specified field reference point.
     *
     * /** Constructor receiving the name of the field and the shape.
     *
     * @param field the name of the field
     * @param shape the shape
     * @return a new geo shape condition builder
     */
    public static GeoShapeConditionBuilder geoShape(String field, GeoShape shape) {
        return new GeoShapeConditionBuilder(field, shape);
    }

    /**
     * Returns a new {@link DateRangeConditionBuilder} with the specified field reference point.
     *
     * @param field the name of the field to be matched
     * @return a new date range condition builder
     */
    public static DateRangeConditionBuilder dateRange(String field) {
        return new DateRangeConditionBuilder(field);
    }

    /**
     * Returns a new {@link SimpleSortFieldBuilder} for the specified field.
     *
     * @param field the name of the field to be sorted by
     * @return a new simple sort field condition builder
     */
    public static SimpleSortFieldBuilder field(String field) {
        return new SimpleSortFieldBuilder(field);
    }

    /**
     * Returns a new {@link SimpleSortFieldBuilder} for the specified field.
     *
     * @param mapper the name of mapper to use to calculate distance
     * @param latitude the latitude of the reference point
     * @param longitude the longitude of the reference point
     * @return a new geo distance sort field builder
     */
    public static GeoDistanceSortFieldBuilder geoDistance(String mapper, double latitude, double longitude) {
        return new GeoDistanceSortFieldBuilder(mapper, latitude, longitude);
    }

    /**
     * Returns a new {@link BitemporalConditionBuilder} for the specified field.
     *
     * @param field the name of the field to be sorted
     * @return a new bitemporal condition builder for the specified field
     */
    public static BitemporalConditionBuilder bitemporal(String field) {
        return new BitemporalConditionBuilder(field);
    }
}
