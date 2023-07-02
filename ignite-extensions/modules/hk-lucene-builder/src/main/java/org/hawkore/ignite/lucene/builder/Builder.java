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
package org.hawkore.ignite.lucene.builder;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.hawkore.ignite.lucene.builder.common.GeoShape;
import org.hawkore.ignite.lucene.builder.common.GeoTransformation;
import org.hawkore.ignite.lucene.builder.index.Index;
import org.hawkore.ignite.lucene.builder.index.Partitioner;
import org.hawkore.ignite.lucene.builder.index.schema.Schema;
import org.hawkore.ignite.lucene.builder.index.schema.analysis.ClasspathAnalyzer;
import org.hawkore.ignite.lucene.builder.index.schema.analysis.SnowballAnalyzer;
import org.hawkore.ignite.lucene.builder.index.schema.mapping.BigDecimalMapper;
import org.hawkore.ignite.lucene.builder.index.schema.mapping.BigIntegerMapper;
import org.hawkore.ignite.lucene.builder.index.schema.mapping.BitemporalMapper;
import org.hawkore.ignite.lucene.builder.index.schema.mapping.BlobMapper;
import org.hawkore.ignite.lucene.builder.index.schema.mapping.BooleanMapper;
import org.hawkore.ignite.lucene.builder.index.schema.mapping.DateMapper;
import org.hawkore.ignite.lucene.builder.index.schema.mapping.DateRangeMapper;
import org.hawkore.ignite.lucene.builder.index.schema.mapping.DoubleMapper;
import org.hawkore.ignite.lucene.builder.index.schema.mapping.FloatMapper;
import org.hawkore.ignite.lucene.builder.index.schema.mapping.GeoPointMapper;
import org.hawkore.ignite.lucene.builder.index.schema.mapping.GeoShapeMapper;
import org.hawkore.ignite.lucene.builder.index.schema.mapping.InetMapper;
import org.hawkore.ignite.lucene.builder.index.schema.mapping.IntegerMapper;
import org.hawkore.ignite.lucene.builder.index.schema.mapping.LongMapper;
import org.hawkore.ignite.lucene.builder.index.schema.mapping.StringMapper;
import org.hawkore.ignite.lucene.builder.index.schema.mapping.TextMapper;
import org.hawkore.ignite.lucene.builder.index.schema.mapping.UUIDMapper;
import org.hawkore.ignite.lucene.builder.search.Search;
import org.hawkore.ignite.lucene.builder.search.condition.AllCondition;
import org.hawkore.ignite.lucene.builder.search.condition.BitemporalCondition;
import org.hawkore.ignite.lucene.builder.search.condition.BooleanCondition;
import org.hawkore.ignite.lucene.builder.search.condition.Condition;
import org.hawkore.ignite.lucene.builder.search.condition.ContainsCondition;
import org.hawkore.ignite.lucene.builder.search.condition.DateRangeCondition;
import org.hawkore.ignite.lucene.builder.search.condition.FuzzyCondition;
import org.hawkore.ignite.lucene.builder.search.condition.GeoBBoxCondition;
import org.hawkore.ignite.lucene.builder.search.condition.GeoDistanceCondition;
import org.hawkore.ignite.lucene.builder.search.condition.GeoShapeCondition;
import org.hawkore.ignite.lucene.builder.search.condition.LuceneCondition;
import org.hawkore.ignite.lucene.builder.search.condition.MatchCondition;
import org.hawkore.ignite.lucene.builder.search.condition.NoneCondition;
import org.hawkore.ignite.lucene.builder.search.condition.PhraseCondition;
import org.hawkore.ignite.lucene.builder.search.condition.PrefixCondition;
import org.hawkore.ignite.lucene.builder.search.condition.RangeCondition;
import org.hawkore.ignite.lucene.builder.search.condition.RegexpCondition;
import org.hawkore.ignite.lucene.builder.search.condition.WildcardCondition;
import org.hawkore.ignite.lucene.builder.search.sort.GeoDistanceSortField;
import org.hawkore.ignite.lucene.builder.search.sort.SimpleSortField;

/**
 * Utility class for creating Lucene index statements.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public abstract class Builder {

    /**
     * Returns a new index creation statement using the session's keyspace.
     *
     * @param table the table name
     * @param name the index name
     * @return a new index creation statement
     */
    public static Index index(String table) {
        return new Index(table);
    }

    /**
     * Returns a new index creation statement using the session's keyspace.
     *
     * @param keyspace the keyspace name
     * @param table the table name
     * @param name the index name
     * @return a new index creation statement
     */
    public static Index index(String keyspace, String table) {
        return new Index(table).keyspace(keyspace);
    }

    /**
     * Returns a new {@link Schema}.
     *
     * @return the {@link Schema}
     */
    public static Schema schema() {
        return new Schema();
    }

    /**
     * Returns a new {@link BigDecimalMapper}.
     *
     * @return a new {@link BigDecimalMapper}
     */
    public static BigDecimalMapper bigDecimalMapper() {
        return new BigDecimalMapper();
    }

    /**
     * Returns a new {@link BigIntegerMapper}.
     *
     * @return a new {@link BigIntegerMapper}
     */
    public static BigIntegerMapper bigIntegerMapper() {
        return new BigIntegerMapper();
    }

    /**
     * Returns a new {@link BitemporalMapper}.
     *
     * @param vtFrom the column name containing the valid time start
     * @param vtTo the column name containing the valid time stop
     * @param ttFrom the column name containing the transaction time start
     * @param ttTo the column name containing the transaction time stop
     * @return a new {@link BitemporalMapper}
     */
    public static BitemporalMapper bitemporalMapper(String vtFrom, String vtTo, String ttFrom, String ttTo) {
        return new BitemporalMapper(vtFrom, vtTo, ttFrom, ttTo);
    }

    /**
     * Returns a new {@link BlobMapper}.
     *
     * @return a new blob mapper
     */
    public static BlobMapper blobMapper() {
        return new BlobMapper();
    }

    /**
     * Returns a new {@link BooleanMapper}.
     *
     * @return a new boolean mapper.
     */
    public static BooleanMapper booleanMapper() {
        return new BooleanMapper();
    }

    /**
     * Returns a new {@link DateMapper}.
     *
     * @return a new date mapper
     */
    public static DateMapper dateMapper() {
        return new DateMapper();
    }

    /**
     * Returns a new {@link DateRangeMapper}.
     *
     * @param from the column containing the start date
     * @param to the column containing the end date
     * @return a new {@link DateRangeMapper}
     */
    public static DateRangeMapper dateRangeMapper(String from, String to) {
        return new DateRangeMapper(from, to);
    }

    /**
     * Returns a new {@link DoubleMapper}.
     *
     * @return a new double mapper
     */
    public static DoubleMapper doubleMapper() {
        return new DoubleMapper();
    }

    /**
     * Returns a new {@link FloatMapper}.
     *
     * @return a new float mapper
     */
    public static FloatMapper floatMapper() {
        return new FloatMapper();
    }

    /**
     * Returns a new {@link GeoPointMapper}.
     *
     * @param latitude the name of the column containing the latitude
     * @param longitude the name of the column containing the longitude
     * @return a new geo point mapper
     */
    public static GeoPointMapper geoPointMapper(String latitude, String longitude) {
        return new GeoPointMapper(latitude, longitude);
    }

    /**
     * Returns a new {@link GeoShapeMapper}.
     *
     * @return a new geo shape mapper
     */
    public static GeoShapeMapper geoShapeMapper() {
        return new GeoShapeMapper();
    }

    /**
     * Returns a new {@link InetMapper}.
     *
     * @return a new inet mapper
     */
    public static InetMapper inetMapper() {
        return new InetMapper();
    }

    /**
     * Returns a new {@link IntegerMapper}.
     *
     * @return a new integer mapper
     */
    public static IntegerMapper integerMapper() {
        return new IntegerMapper();
    }

    /**
     * Returns a new {@link LongMapper}.
     *
     * @return a new long mapper
     */
    public static LongMapper longMapper() {
        return new LongMapper();
    }

    /**
     * Returns a new {@link StringMapper}.
     *
     * @return a new string mapper
     */
    public static StringMapper stringMapper() {
        return new StringMapper();
    }

    /**
     * Returns a new {@link TextMapper}.
     *
     * @return a new text mapper
     */
    public static TextMapper textMapper() {
        return new TextMapper();
    }

    /**
     * Returns a new {@link UUIDMapper}.
     *
     * @return a new UUID mapper
     */
    public static UUIDMapper uuidMapper() {
        return new UUIDMapper();
    }

    /**
     * Returns a new {@link ClasspathAnalyzer}.
     *
     * @param className the Lucene's {@code Analyzer} full class name.
     * @return a new classpath analyzer
     */
    public static ClasspathAnalyzer classpathAnalyzer(String className) {
        return new ClasspathAnalyzer(className);
    }

    /**
     * Returns a new {@link SnowballAnalyzer} for the specified language and stopwords.
     *
     * @param language The language. The supported languages are English, French, Spanish, Portuguese, Italian,
     * Romanian, German, Dutch, Swedish, Norwegian, Danish, Russian, Finnish, Hungarian and Turkish.
     * @return a new snowball analyzer
     */
    public static SnowballAnalyzer snowballAnalyzer(String language) {
        return new SnowballAnalyzer(language);
    }

    /**
     * Returns a new {@link Search}.
     *
     * @return a new search
     */
    public static Search search() {
        return new Search();
    }

    /**
     * Returns a new {@link AllCondition}.
     *
     * @return a new all condition
     */
    public static AllCondition all() {
        return new AllCondition();
    }

    /**
     * Returns a new {@link BitemporalCondition} for the specified field.
     *
     * @param field the name of the field to be sorted
     * @return a new bitemporal condition
     */
    public static BitemporalCondition bitemporal(String field) {
        return new BitemporalCondition(field);
    }

    /**
     * Returns a new {@link BooleanCondition}.
     *
     * @return a new boolean condition
     */
    public static BooleanCondition bool() {
        return new BooleanCondition();
    }

    /**
     * Returns a new {@link BooleanCondition} with the specified mandatory conditions participating in scoring.
     *
     * @param conditions the mandatory conditions
     * @return a new boolean condition with the specified mandatory conditions
     */
    public static BooleanCondition must(Condition<?>... conditions) {
        return bool().must(conditions);
    }

    /**
     * Returns a new {@link BooleanCondition} with the specified optional conditions participating in scoring.
     *
     * @param conditions the optional conditions
     * @return a new boolean condition with the specified optional conditions
     */
    public static BooleanCondition should(Condition<?>... conditions) {
        return bool().should(conditions);
    }

    /**
     * Returns a new {@link BooleanCondition} with the specified mandatory not conditions not participating in scoring.
     *
     * @param conditions the mandatory not conditions
     * @return a new boolean condition with the specified mandatory not conditions
     */
    public static BooleanCondition not(Condition<?>... conditions) {
        return bool().not(conditions);
    }

    /**
     * Returns a new {@link ContainsCondition}.
     *
     * @param field the name of the field to be matched
     * @param values the values of the field to be matched
     * @return a new contains condition
     */
    public static ContainsCondition contains(String field, Object... values) {
        return new ContainsCondition(field, values);
    }

    /**
     * Returns a new {@link FuzzyCondition} for the specified field and value.
     *
     * @param field the name of the field to be matched
     * @param value the value of the field to be matched
     * @return a new fuzzy condition
     */
    public static FuzzyCondition fuzzy(String field, String value) {
        return new FuzzyCondition(field, value);
    }

    /**
     * Returns a new {@link LuceneCondition} with the specified query.
     *
     * @param query the Lucene syntax query
     * @return a new Lucene condition
     */
    public static LuceneCondition lucene(String query) {
        return new LuceneCondition(query);
    }

    /**
     * Returns a new {@link MatchCondition} for the specified field and value.
     *
     * @param field the name of the field to be matched
     * @param value the value of the field to be matched
     * @return a new match condition
     */
    public static MatchCondition match(String field, Object value) {
        return new MatchCondition(field, value);
    }

    /**
     * Returns a new {@link NoneCondition}.
     *
     * @return a new none condition
     */
    public static NoneCondition none() {
        return new NoneCondition();
    }

    /**
     * Returns a new {@link PhraseCondition} for the specified field and values.
     *
     * @param field the name of the field to be matched
     * @param value the text to be matched
     * @return a new phrase condition
     */
    public static PhraseCondition phrase(String field, String value) {
        return new PhraseCondition(field, value);
    }

    /**
     * Returns a new {@link PrefixCondition} for the specified field and value.
     *
     * @param field the name of the field to be matched
     * @param value the value of the field to be matched
     * @return a new prefix condition
     */
    public static PrefixCondition prefix(String field, String value) {
        return new PrefixCondition(field, value);
    }

    /**
     * Returns a new {@link RangeCondition} for the specified field.
     *
     * @param field the name of the field to be matched
     * @return a new range condition
     */
    public static RangeCondition range(String field) {
        return new RangeCondition(field);
    }

    /**
     * Returns a new {@link RegexpCondition} for the specified field and value.
     *
     * @param field the name of the field to be matched
     * @param value the value of the field to be matched
     * @return a new regexp condition
     */
    public static RegexpCondition regexp(String field, String value) {
        return new RegexpCondition(field, value);
    }

    /**
     * Returns a new {@link WildcardCondition} for the specified field and value.
     *
     * @param field the name of the field to be matched
     * @param value the value of the field to be matched
     * @return a new wildcard condition
     */
    public static WildcardCondition wildcard(String field, String value) {
        return new WildcardCondition(field, value);
    }

    /**
     * Returns a new {@link GeoBBoxCondition} with the specified field name and bounding box coordinates.
     *
     * @param field the name of the field to be matched
     * @param minLongitude the minimum accepted longitude
     * @param maxLongitude the maximum accepted longitude
     * @param minLatitude the minimum accepted latitude
     * @param maxLatitude the maximum accepted latitude
     * @return a new geo bounding box condition
     */
    public static GeoBBoxCondition geoBBox(String field,
                                           double minLatitude,
                                           double maxLatitude,
                                           double minLongitude,
                                           double maxLongitude) {
        return new GeoBBoxCondition(field, minLatitude, maxLatitude, minLongitude, maxLongitude);
    }

    /**
     * Returns a new {@link GeoDistanceCondition} with the specified field reference point.
     *
     * @param field the name of the field to be matched
     * @param latitude the latitude of the reference point
     * @param longitude the longitude of the reference point
     * @param maxDistance the max allowed distance
     * @return a new geo distance condition
     */
    public static GeoDistanceCondition geoDistance(String field,
                                                   double latitude,
                                                   double longitude,
                                                   String maxDistance) {
        return new GeoDistanceCondition(field, latitude, longitude, maxDistance);
    }

    /**
     * Returns a new {@link GeoShapeCondition} with the specified shape.
     *
     * @param field the name of the field
     * @param shape the shape
     * @return a new geo shape condition
     */
    public static GeoShapeCondition geoShape(String field, GeoShape shape) {
        return new GeoShapeCondition(field, shape);
    }

    /**
     * Returns a new {@link GeoShapeCondition} with the specified shape.
     *
     * @param field the name of the field
     * @param shape the shape in WKT format
     * @return a new geo shape condition
     */
    public static GeoShapeCondition geoShape(String field, String shape) {
        return geoShape(field, wkt(shape));
    }

    /**
     * Returns a new {@link DateRangeCondition} with the specified field reference point.
     *
     * @param field the name of the field to be matched
     * @return a new date range condition
     */
    public static DateRangeCondition dateRange(String field) {
        return new DateRangeCondition(field);
    }

    /**
     * Returns a new {@link SimpleSortField} for the specified field.
     *
     * @param field the name of the field to be sorted
     * @return a new simple sort field
     */
    public static SimpleSortField field(String field) {
        return new SimpleSortField(field);
    }

    /**
     * Returns a new {@link GeoDistanceSortField} for the specified field.
     *
     * @param field the name of the geo point field mapper to be used for sorting
     * @param latitude the latitude in degrees of the point to min distance sort by
     * @param longitude the longitude in degrees of the point to min distance sort by
     * @return a new geo distance sort field
     */
    public static GeoDistanceSortField geoDistance(String field, double latitude, double longitude) {
        return new GeoDistanceSortField(field, latitude, longitude);
    }

    /**
     * Returns a new {@link GeoTransformation.BBox} transformation to be applied to {@link GeoShapeMapper}s.
     *
     * @return a new bbox transformation
     */
    public static GeoTransformation.BBox bbox() {
        return new GeoTransformation.BBox();
    }

    /**
     * Returns a new {@link GeoTransformation.Buffer} transformation to be applied to {@link GeoShapeMapper}s.
     *
     * @return a new buffer transformation
     */
    public static GeoTransformation.Buffer buffer() {
        return new GeoTransformation.Buffer();
    }

    /**
     * Returns a new {@link GeoTransformation.Centroid} transformation to be applied to {@link GeoShapeMapper}s.
     *
     * @return a new centroid transformation
     */
    public static GeoTransformation.Centroid centroid() {
        return new GeoTransformation.Centroid();
    }

    /**
     * Returns a new {@link GeoTransformation.Centroid} transformation to be applied to {@link GeoShapeMapper}s.
     *
     * @return a new convex hull transformation
     */
    public static GeoTransformation.ConvexHull convexHull() {
        return new GeoTransformation.ConvexHull();
    }

    /**
     * Returns a new {@link GeoShape.WKT}.
     *
     * @param value the WKT string value
     * @return a new bbox transformation
     */
    public static GeoShape.WKT wkt(String value) {
        return new GeoShape.WKT(value);
    }

    /**
     * Returns a new {@link GeoShape.BBox}, representing the bounding box around the specified {@link GeoShape}.
     *
     * @param shape the base shape
     * @return a new bbox transformation
     */
    public static GeoShape.BBox bbox(GeoShape shape) {
        return new GeoShape.BBox(shape);
    }

    /**
     * Returns a new {@link GeoShape.BBox}, representing the bounding box around the specified WKT shape.
     *
     * @param shape the base shape in WKT format
     * @return a new bbox transformation
     */
    public static GeoShape.BBox bbox(String shape) {
        return bbox(wkt(shape));
    }

    /**
     * Returns a new {@link GeoShape.Buffer}, representing a buffer around the specified {@link GeoShape}.
     *
     * @param shape the base shape
     * @return a new buffer transformation
     */
    public static GeoShape.Buffer buffer(GeoShape shape) {
        return new GeoShape.Buffer(shape);
    }

    /**
     * Returns a new {@link GeoShape.Buffer}, representing a buffer around the specified WKT shape.
     *
     * @param shape the base shape in WKT format
     * @return a new buffer transformation
     */
    public static GeoShape.Buffer buffer(String shape) {
        return buffer(wkt(shape));
    }

    /**
     * Returns a new {@link GeoShape.Centroid}, representing the centroid of the specified {@link GeoShape}.
     *
     * @param shape the base shape
     * @return a new centroid transformation
     */
    public static GeoShape.Centroid centroid(GeoShape shape) {
        return new GeoShape.Centroid(shape);
    }

    /**
     * Returns a new {@link GeoShape.Centroid}, representing the centroid of the specified WKT shape.
     *
     * @param shape the base shape in WKT format
     * @return a new centroid transformation
     */
    public static GeoShape.Centroid centroid(String shape) {
        return centroid(wkt(shape));
    }

    /**
     * Returns a new {@link GeoShape.ConvexHull}, representing the convex hull of the specified {@link GeoShape}.
     *
     * @param shape the base shape
     * @return a new convex hull transformation
     */
    public static GeoShape.ConvexHull convexHull(GeoShape shape) {
        return new GeoShape.ConvexHull(shape);
    }

    /**
     * Returns a new {@link GeoShape.ConvexHull}, representing the convex hull of the specified WKT shape.
     *
     * @param shape the base shape in WKT format
     * @return a new convex hull transformation
     */
    public static GeoShape.ConvexHull convexHull(String shape) {
        return convexHull(wkt(shape));
    }

    /**
     * Returns a new empty {@link GeoShape.Difference}.
     *
     * @return a new difference transformation
     */
    public static GeoShape.Difference difference() {
        return new GeoShape.Difference();
    }

    /**
     * Returns a new {@link GeoShape.Difference}, representing the difference of the specified {@link GeoShape}s.
     *
     * @param shapes the shapes to be subtracted
     * @return a new difference transformation
     */
    public static GeoShape.Difference difference(GeoShape... shapes) {
        return new GeoShape.Difference(shapes);
    }

    /**
     * Returns a new {@link GeoShape.Difference}, representing the difference of the specified {@link GeoShape}s.
     *
     * @param shapes the shapes to be subtracted
     * @return a new difference transformation
     */
    public static GeoShape.Difference difference(List<GeoShape> shapes) {
        return new GeoShape.Difference(shapes);
    }

    /**
     * Returns a new {@link GeoShape.Difference}, representing the difference of the specified WKT shapes.
     *
     * @param shapes the shapes to be subtracted in WKT format
     * @return a new difference transformation
     */
    public static GeoShape.Difference difference(String... shapes) {
        return difference(Stream.of(shapes).map(Builder::wkt).collect(Collectors.toList()));
    }

    /**
     * Returns a new empty {@link GeoShape.Intersection}.
     *
     * @return a new intersection transformation
     */
    public static GeoShape.Intersection intersection() {
        return new GeoShape.Intersection();
    }

    /**
     * Returns a new {@link GeoShape.Intersection}, representing the intersection of the specified {@link GeoShape}s.
     *
     * @param shapes the shapes to be intersected
     * @return a new intersection transformation
     */
    public static GeoShape.Intersection intersection(GeoShape... shapes) {
        return new GeoShape.Intersection(shapes);
    }

    /**
     * Returns a new {@link GeoShape.Intersection}, representing the intersection of the specified {@link GeoShape}s.
     *
     * @param shapes the shapes to be intersected
     * @return a new intersection transformation
     */
    public static GeoShape.Intersection intersection(List<GeoShape> shapes) {
        return new GeoShape.Intersection(shapes);
    }

    /**
     * Returns a new {@link GeoShape.Intersection}, representing the intersection of the specified WKT shapes.
     *
     * @param shapes the shapes to be intersected
     * @return a new intersection transformation
     */
    public static GeoShape.Intersection intersection(String... shapes) {
        return intersection(Stream.of(shapes).map(Builder::wkt).collect(Collectors.toList()));
    }

    /**
     * Returns a new empty {@link GeoShape.Union}.
     *
     * @return a new union transformation
     */
    public static GeoShape.Union union() {
        return new GeoShape.Union();
    }

    /**
     * Returns a new {@link GeoShape.Union}, representing the union of the specified {@link GeoShape}s.
     *
     * @param shapes the shapes to be added
     * @return a new union transformation
     */
    public static GeoShape.Union union(GeoShape... shapes) {
        return new GeoShape.Union(shapes);
    }

    /**
     * Returns a new {@link GeoShape.Union}, representing the union of the specified {@link GeoShape}s.
     *
     * @param shapes the shapes to be added
     * @return a new union transformation
     */
    public static GeoShape.Union union(List<GeoShape> shapes) {
        return new GeoShape.Union(shapes);
    }

    /**
     * Returns a new {@link GeoShape.Union}, representing the union of the specified WKT shapes.
     *
     * @param shapes the shapes to be subtracted in WKT format
     * @return a new difference transformation
     */
    public static GeoShape.Union union(String... shapes) {
        return union(Stream.of(shapes).map(Builder::wkt).collect(Collectors.toList()));
    }

    /**
     * Returns a new {@link Partitioner.None} to not partitioning the index.
     *
     * Index partitioning is useful to speed up some queries to the detriment of others, depending on the
     * implementation. It is also useful to overcome the Lucene's hard limit of 2147483519 documents per index.
     *
     * @return a new no-action partitioning, equivalent to just don't partitioning the index
     */
    public static Partitioner.None nonePartitioner() {
        return new Partitioner.None();
    }

    /**
     * Returns a new {@link Partitioner.OnToken} based on the partition key token. Rows will be stored in an index
     * partition determined by the hash of the partition key token. Partition-directed searches will be routed to a
     * single partition, increasing performance. However, token range searches will be routed to all the partitions,
     * with a slightly lower performance.
     *
     * This partitioner guarantees an excellent load balancing between index partitions.
     *
     * @param partitions the number of index partitions per node
     * @return a new partitioner based on QueryEntity's partitioning token
     */
    public static Partitioner.OnToken partitionerOnToken(int partitions) {
        return new Partitioner.OnToken(partitions);
    }
}
