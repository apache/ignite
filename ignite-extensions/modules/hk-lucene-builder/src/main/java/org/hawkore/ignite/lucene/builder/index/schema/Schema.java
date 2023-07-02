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
package org.hawkore.ignite.lucene.builder.index.schema;

import java.util.LinkedHashMap;
import java.util.Map;

import org.hawkore.ignite.lucene.builder.JSONBuilder;
import org.hawkore.ignite.lucene.builder.index.schema.analysis.Analyzer;
import org.hawkore.ignite.lucene.builder.index.schema.mapping.Mapper;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The user-defined mapping from QueryEntity columns to Lucene documents.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class Schema extends JSONBuilder {

    /** The default analyzer. */
    @JsonProperty("default_analyzer")
    public String defaultAnalyzerName;

    /** The analyzers. */
    @JsonProperty("analyzers")
    public Map<String, Analyzer> analyzers;

    /** The mappers. */
    @JsonProperty("fields")
    public Map<String, Mapper> mappers;

    /**
     * Sets the name of the default {@link Analyzer}.
     *
     * @param name the name of the default {@link Analyzer}
     * @return this with the specified default analyzer
     */
    public Schema defaultAnalyzer(String name) {
        defaultAnalyzerName = name;
        return this;
    }

    /**
     * Adds a new {@link Analyzer}.
     *
     * @param name the name of the {@link Analyzer} to be added
     * @param analyzer the {@link Analyzer} to be added
     * @return this with the specified analyzer
     */
    public Schema analyzer(String name, Analyzer analyzer) {
        if (analyzers == null) {
            analyzers = new LinkedHashMap<>();
        }
        analyzers.put(name, analyzer);
        return this;
    }

    /**
     * Adds a new {@link Mapper}.
     *
     * @param field the name of the {@link Mapper} to be added
     * @param mapper the {@link Mapper} to be added
     * @return this with the specified mapper
     */
    public Schema mapper(String field, Mapper mapper) {
        if (mappers == null) {
            mappers = new LinkedHashMap<>();
        }
        mappers.put(field, mapper);
        return this;
    }
}
