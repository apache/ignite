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

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.hawkore.ignite.lucene.IndexException;
import org.hawkore.ignite.lucene.common.JsonSerializer;
import org.hawkore.ignite.lucene.schema.analysis.AccentInsensitiveAnalyzer;
import org.hawkore.ignite.lucene.schema.analysis.AnalyzerBuilder;
import org.hawkore.ignite.lucene.schema.analysis.ClasspathAnalyzerBuilder;
import org.hawkore.ignite.lucene.schema.analysis.StandardAnalyzers;
import org.hawkore.ignite.lucene.schema.mapping.Mapper;
import org.hawkore.ignite.lucene.schema.mapping.builder.MapperBuilder;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Class for building new {@link Schema}s both programmatically and from JSON.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class SchemaBuilder {

    @JsonProperty("default_analyzer")
    private String defaultAnalyzerName;

    @JsonProperty("analyzers")
    private final Map<String, AnalyzerBuilder> analyzerBuilders;

    @JsonProperty("fields")
    private final Map<String, MapperBuilder<?, ?>> mapperBuilders;

    @JsonCreator
    SchemaBuilder(@JsonProperty("default_analyzer") String defaultAnalyzerName,
                  @JsonProperty("analyzers") Map<String, AnalyzerBuilder> analyzerBuilders,
                  @JsonProperty("fields") Map<String, MapperBuilder<?, ?>> mapperBuilders) {
        this.defaultAnalyzerName = defaultAnalyzerName;
        this.analyzerBuilders = analyzerBuilders != null ? analyzerBuilders : new LinkedHashMap<>();
        this.mapperBuilders = mapperBuilders != null ? mapperBuilders : new LinkedHashMap<>();
    }

    /**
     * Sets the name of the default {@link Analyzer}.
     *
     * @param name the name of the default {@link Analyzer}
     * @return this with the specified default analyzer
     */
    public SchemaBuilder defaultAnalyzer(String name) {
        defaultAnalyzerName = name;
        return this;
    }

    /**
     * Adds a new {@link Analyzer}.
     *
     * @param name the name of the {@link Analyzer} to be added
     * @param analyzer the builder of the {@link Analyzer} to be added
     * @return this with the specified analyzer
     */
    public SchemaBuilder analyzer(String name, AnalyzerBuilder analyzer) {
        analyzerBuilders.put(name, analyzer);
        return this;
    }

    /**
     * Adds a new {@link Mapper}.
     *
     * @param field the name of the {@link Mapper} to be added
     * @param mapper the builder of the {@link Mapper} to be added
     * @return this with the specified mapper
     */
    public SchemaBuilder mapper(String field, MapperBuilder<?, ?> mapper) {
        mapperBuilders.put(field, mapper);
        return this;
    }

    /**
     * Returns the {@link Schema} defined by this.
     *
     * @return a new schema
     */
    public Schema build() {

        Map<String, Mapper> mappers = new LinkedHashMap<>(mapperBuilders.size());
        for (Map.Entry<String, MapperBuilder<?, ?>> entry : mapperBuilders.entrySet()) {
            String name = entry.getKey();
            MapperBuilder<?, ?> builder = entry.getValue();
            Mapper mapper = builder.build(name);
            mappers.put(name, mapper);
        }

        Map<String, Analyzer> analyzers = new LinkedHashMap<>();
        for (Map.Entry<String, AnalyzerBuilder> entry : analyzerBuilders.entrySet()) {
            String name = entry.getKey();
            Analyzer analyzer = entry.getValue().analyzer();
            analyzers.put(name, analyzer);
        }

        Analyzer defaultAnalyzer;
        if (defaultAnalyzerName == null) {
            defaultAnalyzer = StandardAnalyzers.DEFAULT.get();
        } else {
            defaultAnalyzer = analyzers.get(defaultAnalyzerName);
            if (defaultAnalyzer == null) {
                defaultAnalyzer = StandardAnalyzers.get(defaultAnalyzerName);
                if (defaultAnalyzer == null) {
                    try {
                        defaultAnalyzer = (new ClasspathAnalyzerBuilder(defaultAnalyzerName)).analyzer();
                    } catch (Exception e) {
                        throw new IndexException(e, "Not found analyzer: '{}'", defaultAnalyzerName);
                    }
                }
                analyzers.put(defaultAnalyzerName, defaultAnalyzer);
            }
        }
        return new Schema(new AccentInsensitiveAnalyzer(defaultAnalyzer), mappers, analyzers);
    }

    /**
     * Returns the JSON representation of this builder.
     *
     * @return a JSON {@code String}
     */
    public String toJson() {
        try {
            return JsonSerializer.toString(this);
        } catch (IOException e) {
            throw new IndexException(e, "Unformateable JSON schema: {}", e.getMessage());
        }
    }

    /**
     * Returns the {@link Schema} contained in the specified JSON {@code String}.
     *
     * @param json a {@code String} containing the JSON representation of the {@link Schema} to be parsed
     * @return the schema contained in the specified JSON {@code String}
     */
    public static SchemaBuilder fromJson(String json) {
        try {
            return JsonSerializer.fromString(json, SchemaBuilder.class);
        } catch (IOException e) {
            throw new IndexException(e, "Unparseable JSON schema: {}: {}", e.getMessage(), json);
        }
    }

    /**
     * @return the defaultAnalyzerName
     */
    public String getDefaultAnalyzerName() {
        return defaultAnalyzerName;
    }

    /**
     * @return the analyzerBuilders
     */
    public Map<String, AnalyzerBuilder> getAnalyzerBuilders() {
        return analyzerBuilders;
    }

    /**
     * @return the mapperBuilders
     */
    public Map<String, MapperBuilder<?, ?>> getMapperBuilders() {
        return mapperBuilders;
    }

}
