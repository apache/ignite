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
package org.hawkore.ignite.lucene.schema;

import java.util.LinkedHashMap;

import org.hawkore.ignite.lucene.schema.analysis.ClasspathAnalyzerBuilder;
import org.hawkore.ignite.lucene.schema.analysis.SnowballAnalyzerBuilder;
import org.hawkore.ignite.lucene.schema.mapping.builder.BigDecimalMapperBuilder;
import org.hawkore.ignite.lucene.schema.mapping.builder.BigIntegerMapperBuilder;
import org.hawkore.ignite.lucene.schema.mapping.builder.BitemporalMapperBuilder;
import org.hawkore.ignite.lucene.schema.mapping.builder.BlobMapperBuilder;
import org.hawkore.ignite.lucene.schema.mapping.builder.BooleanMapperBuilder;
import org.hawkore.ignite.lucene.schema.mapping.builder.DateMapperBuilder;
import org.hawkore.ignite.lucene.schema.mapping.builder.DateRangeMapperBuilder;
import org.hawkore.ignite.lucene.schema.mapping.builder.DoubleMapperBuilder;
import org.hawkore.ignite.lucene.schema.mapping.builder.FloatMapperBuilder;
import org.hawkore.ignite.lucene.schema.mapping.builder.GeoPointMapperBuilder;
import org.hawkore.ignite.lucene.schema.mapping.builder.GeoShapeMapperBuilder;
import org.hawkore.ignite.lucene.schema.mapping.builder.InetMapperBuilder;
import org.hawkore.ignite.lucene.schema.mapping.builder.IntegerMapperBuilder;
import org.hawkore.ignite.lucene.schema.mapping.builder.LongMapperBuilder;
import org.hawkore.ignite.lucene.schema.mapping.builder.StringMapperBuilder;
import org.hawkore.ignite.lucene.schema.mapping.builder.TextMapperBuilder;
import org.hawkore.ignite.lucene.schema.mapping.builder.UUIDMapperBuilder;

/**
 * Class centralizing several {@link Schema} related builders.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public final class SchemaBuilders {

    /** Private constructor to hide the implicit public one. */
    private SchemaBuilders() {
    }

    /**
     * Returns a new {@link SchemaBuilder}.
     *
     * @return a new schema builder
     */
    public static SchemaBuilder schema() {
        return new SchemaBuilder(null, new LinkedHashMap<>(), new LinkedHashMap<>());
    }

    /**
     * Returns a new {@link BigDecimalMapperBuilder}.
     *
     * @return a new big decimal mapper builder
     */
    public static BigDecimalMapperBuilder bigDecimalMapper() {
        return new BigDecimalMapperBuilder();
    }

    /**
     * Returns a new {@link BigIntegerMapperBuilder}.
     *
     * @return a new big integer mapper builder
     */
    public static BigIntegerMapperBuilder bigIntegerMapper() {
        return new BigIntegerMapperBuilder();
    }

    /**
     * Returns a new {@link BitemporalMapperBuilder}.
     *
     * @param vtFrom the column name containing the valid time start
     * @param vtTo the column name containing the valid time stop
     * @param ttFrom the column name containing the transaction time start
     * @param ttTo the column name containing the transaction time stop
     * @return a new bitemporal mapper builder
     */
    public static BitemporalMapperBuilder bitemporalMapper(String vtFrom, String vtTo, String ttFrom, String ttTo) {
        return new BitemporalMapperBuilder(vtFrom, vtTo, ttFrom, ttTo);
    }

    /**
     * Returns a new {@link BlobMapperBuilder}.
     *
     * @return a new blob mapper builder
     */
    public static BlobMapperBuilder blobMapper() {
        return new BlobMapperBuilder();
    }

    /**
     * Returns a new {@link BooleanMapperBuilder}.
     *
     * @return a new boolean mapper builder
     */
    public static BooleanMapperBuilder booleanMapper() {
        return new BooleanMapperBuilder();
    }

    /**
     * Returns a new {@link DateMapperBuilder}.
     *
     * @return a new decimal mapper builder
     */
    public static DateMapperBuilder dateMapper() {
        return new DateMapperBuilder();
    }

    /**
     * Returns a new {@link DateRangeMapperBuilder}.
     *
     * @param from the name of the column containing the start date
     * @param to the name of the column containing the end date
     * @return a new date range mapper builder
     */
    public static DateRangeMapperBuilder dateRangeMapper(String from, String to) {
        return new DateRangeMapperBuilder(from, to);
    }

    /**
     * Returns a new {@link DoubleMapperBuilder}.
     *
     * @return a new double mapper builder
     */
    public static DoubleMapperBuilder doubleMapper() {
        return new DoubleMapperBuilder();
    }

    /**
     * Returns a new {@link FloatMapperBuilder}.
     *
     * @return a new float mapper builder
     */
    public static FloatMapperBuilder floatMapper() {
        return new FloatMapperBuilder();
    }

    /**
     * Returns a new {@link GeoPointMapperBuilder}.
     *
     * @param latitude the name of the column containing the latitude
     * @param longitude the name of the column containing the longitude
     * @return a new geo point mapper builder
     */
    public static GeoPointMapperBuilder geoPointMapper(String latitude, String longitude) {
        return new GeoPointMapperBuilder(latitude, longitude);
    }

    /**
     * Returns a new {@link GeoShapeMapperBuilder}.
     *
     * @return a new geo shape mapper builder
     */
    public static GeoShapeMapperBuilder geoShapeMapper() {
        return new GeoShapeMapperBuilder();
    }

    /**
     * Returns a new {@link InetMapperBuilder}.
     *
     * @return a new inet mapper builder
     */
    public static InetMapperBuilder inetMapper() {
        return new InetMapperBuilder();
    }

    /**
     * Returns a new {@link IntegerMapperBuilder}.
     *
     * @return a new integer mapper builder
     */
    public static IntegerMapperBuilder integerMapper() {
        return new IntegerMapperBuilder();
    }

    /**
     * Returns a new {@link LongMapperBuilder}.
     *
     * @return a new long mapper builder
     */
    public static LongMapperBuilder longMapper() {
        return new LongMapperBuilder();
    }

    /**
     * Returns a new {@link StringMapperBuilder}.
     *
     * @return a new string mapper builder
     */
    public static StringMapperBuilder stringMapper() {
        return new StringMapperBuilder();
    }

    /**
     * Returns a new {@link TextMapperBuilder}.
     *
     * @return a new text mapper builder
     */
    public static TextMapperBuilder textMapper() {
        return new TextMapperBuilder();
    }

    /**
     * Returns a new {@link UUIDMapperBuilder}.
     *
     * @return a new UUID mapper builder
     */
    public static UUIDMapperBuilder uuidMapper() {
        return new UUIDMapperBuilder();
    }

    /**
     * Returns a new {@link ClasspathAnalyzerBuilder}.
     *
     * @param className the {@link org.apache.lucene.analysis.Analyzer} full class name
     * @return a new classpath analyzer builder
     */
    public static ClasspathAnalyzerBuilder classpathAnalyzer(String className) {
        return new ClasspathAnalyzerBuilder(className);
    }

    /**
     * Returns a new {@link SnowballAnalyzerBuilder} for the specified language and stopwords.
     *
     * @param language The language. The supported languages are English, French, Spanish, Portuguese, Italian,
     * Romanian, German, Dutch, Swedish, Norwegian, Danish, Russian, Finnish, Irish, Hungarian, Turkish, Armenian,
     * Basque and Catalan.
     * @param stopwords the comma separated stopwords {@code String} list
     * @return a new snowball analyzer builder
     */
    public static SnowballAnalyzerBuilder snowballAnalyzer(String language, String stopwords) {
        return new SnowballAnalyzerBuilder(language, stopwords);
    }

}
