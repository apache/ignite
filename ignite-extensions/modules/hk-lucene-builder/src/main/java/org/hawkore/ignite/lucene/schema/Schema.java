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

import java.io.Closeable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexableField;
import org.hawkore.ignite.lucene.column.Column;
import org.hawkore.ignite.lucene.column.Columns;
import org.hawkore.ignite.lucene.schema.mapping.Mapper;
import org.hawkore.ignite.lucene.search.Search;

import com.google.common.base.MoreObjects;

/**
 * The user-defined mapping from QueryEntity columns to Lucene documents.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class Schema implements Closeable {

    /** The {@link Columns} {@link Mapper}s. */
    public final Map<String, Mapper> mappers;

    /** The wrapping all-in-one {@link Analyzer}. */
    public final SchemaAnalyzer analyzer;

    /** The default {@link Analyzer}. */
    public final Analyzer defaultAnalyzer;

    /** The names of the mapped cells. */
    public final Set<String> mappedCells;

    /**
     * Returns a new {@code Schema} for the specified {@link Mapper}s and {@link Analyzer}s.
     *
     * @param defaultAnalyzer the default {@link Analyzer} to be used
     * @param mappers the per field {@link Mapper}s builders to be used
     * @param analyzers the per field {@link Analyzer}s to be used
     */
    public Schema(Analyzer defaultAnalyzer, Map<String, Mapper> mappers, Map<String, Analyzer> analyzers) {
        this.mappers = mappers;
        this.defaultAnalyzer = defaultAnalyzer;
        this.analyzer = new SchemaAnalyzer(defaultAnalyzer, analyzers, mappers);
        mappedCells = mappers.values()
                             .stream()
                             .flatMap(x -> x.mappedColumns.stream())
                             .map(Column::parseCellName)
                             .collect(Collectors.toSet());
    }

    /**
     * Returns the {@link Analyzer} identified by the specified field name.
     *
     * @param fieldName a field name
     * @return an {@link Analyzer}
     */
    public Analyzer analyzer(String fieldName) {
        return analyzer.getAnalyzer(fieldName).analyzer();
    }

    /**
     * Returns the {@link Mapper} identified by the specified field name, or {@code null} if not found.
     *
     * @param field a field name
     * @return the mapper, or {@code null} if not found.
     */
    public Mapper mapper(String field) {
        String mapperName = Column.parseMapperName(field);
        return mappers.get(mapperName);
    }

    /**
     * Returns the names of the cells mapped by the mappers.
     *
     * @return the names of the mapped cells
     */
    public Set<String> mappedCells() {
        return mappedCells;
    }

    /**
     * Validates the specified {@link Columns} for mapping.
     *
     * @param columns the {@link Columns} to be validated
     */
    public void validate(Columns columns) {
        for (Mapper mapper : mappers.values()) {
            mapper.validate(columns);
        }
    }

    /**
     * Returns the Lucene {@link IndexableField}s resulting from the mapping of the specified {@link Columns}. <p> This
     * is done in a best-effort way, so each mapper errors are logged and ignored.
     *
     * @param columns the {@link Columns} to be added
     * @return a list of indexable fields
     */
    public List<IndexableField> indexableFields(Columns columns) {
        List<IndexableField> fields = new LinkedList<>();
        mappers.values().forEach(mapper -> fields.addAll(mapper.bestEffortIndexableFields(columns)));
        return fields;
    }

    /**
     * Returns the Lucene {@link IndexableField}s resulting from the mapping of the specified {@link Columns} only if
     * they are required by the post processing phase of the specified {@link Search}.
     *
     * @param columns the {@link Columns} to be added
     * @param search a search
     * @return a list of indexable fields
     */
    public List<IndexableField> postProcessingIndexableFields(Columns columns, Search search) {
        List<IndexableField> fields = new LinkedList<>();
        search.postProcessingFields().forEach(field -> {
            Mapper mapper = mapper(field);
            if (mapper != null) {
                fields.addAll(mapper.indexableFields(columns));
            }
        });
        return fields;
    }

    /**
     * Returns if this has any mapping for the specified cell.
     *
     * @param cell the cell name
     * @return {@code true} if there is any mapping for the cell, {@code false} otherwise
     */
    public boolean mapsCell(String cell) {
        return mappers.values().stream().anyMatch(mapper -> mapper.mapsCell(cell));
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        analyzer.close();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("mappers", mappers).add("analyzer", analyzer).toString();
    }
}
