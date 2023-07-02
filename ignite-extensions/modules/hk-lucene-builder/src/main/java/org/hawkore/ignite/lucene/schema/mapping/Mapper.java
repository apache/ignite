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
package org.hawkore.ignite.lucene.schema.mapping;

import static java.util.stream.Collectors.toList;

import java.math.BigInteger;
import java.net.InetAddress;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BytesRef;
import org.hawkore.ignite.lucene.IndexException;
import org.hawkore.ignite.lucene.column.Column;
import org.hawkore.ignite.lucene.column.Columns;
import org.hawkore.ignite.lucene.schema.analysis.StandardAnalyzers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.MoreObjects;

/**
 * Class for mapping between QueryEntity's columns and Lucene documents.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public abstract class Mapper {

    private static final Logger logger = LoggerFactory.getLogger(Mapper.class);

    /** A no-action analyzer for not tokenized {@link Mapper} implementations. */
    static final String KEYWORD_ANALYZER = StandardAnalyzers.KEYWORD.toString();

    static final List<Class<?>> TEXT_TYPES = Collections.singletonList(String.class);

    static final List<Class<?>> INTEGER_TYPES = Arrays.asList(
            String.class, Byte.class, Short.class, Integer.class, Long.class, BigInteger.class);

    static final List<Class<?>> NUMERIC_TYPES = Arrays.asList(String.class, Number.class);

    static final List<Class<?>> DATE_TYPES = Arrays.asList(
            String.class, Integer.class, Long.class, BigInteger.class, Date.class, UUID.class,
            LocalDateTime.class, LocalDate.class, OffsetDateTime.class, ZonedDateTime.class,
            Instant.class, OffsetTime.class, LocalTime.class
        );

    static final List<Class<?>> NUMERIC_TYPES_WITH_DATE = Arrays.asList(String.class, Number.class, Date.class);

    static final List<Class<?>> PRINTABLE_TYPES = Arrays.asList(
            String.class, Number.class, UUID.class, Boolean.class, InetAddress.class);

    static final List<Class<?>> EMPTY_TYPE_LIST = Collections.emptyList();

    /** The store field in Lucene default option. */
    static final Store STORE = Store.NO;

    /** If the field must be validated when no specified. */
    static final boolean DEFAULT_VALIDATED = false;

    /** The name of the Lucene field. */
    public final String field;

    /** If the field produces doc values. */
    public final Boolean docValues;

    /** If the field must be validated. */
    public final Boolean validated;

    /** The name of the analyzer to be used. */
    public final String analyzer;

    /** The names of the columns to be mapped. */
    public final List<String> mappedColumns;

    /** The names of the columns to be mapped. */
    public final List<String> mappedCells;

    /** The supported column value data types. */
    public final List<Class<?>> supportedTypes;

    /** The explicitly excluded column value data types. */
    public final List<Class<?>> excludedTypes;

    /** If this mapper support collections. */
    public final Boolean supportsCollections;

    /**
     * Builds a new {@link Mapper} supporting the specified types for indexing.
     *
     * @param field the name of the field
     * @param docValues if the mapper supports doc values
     * @param validated if the field must be validated
     * @param analyzer the name of the analyzer to be used
     * @param mappedColumns the names of the columns to be mapped
     * @param supportedTypes the supported column value data types
     * @param excludedTypes the explicitly excluded value data types
     * @param supportsCollections if this mapper supports collections
     */
    protected Mapper(String field,
                     Boolean docValues,
                     Boolean validated,
                     String analyzer,
                     List<String> mappedColumns,
                     List<Class<?>> supportedTypes,
                     List<Class<?>> excludedTypes,
                     Boolean supportsCollections) {
        if (StringUtils.isBlank(field)) {
            throw new IndexException("Field name is required");
        }
        this.field = field;
        this.docValues = docValues;
        this.validated = validated == null ? DEFAULT_VALIDATED : validated;
        this.analyzer = analyzer;
        this.mappedColumns = mappedColumns.stream().filter(Objects::nonNull).collect(toList()); // Remove nulls
        this.mappedCells = this.mappedColumns.stream().map(Column::parseCellName).collect(toList());
        this.supportedTypes = supportedTypes;
        this.excludedTypes= excludedTypes;
        this.supportsCollections = supportsCollections;
    }

    /**
     * Returns the Lucene {@link IndexableField}s resulting from the mapping of the specified {@link Columns}.
     *
     * @param columns the columns
     * @return a list of indexable fields
     */
    public abstract List<IndexableField> indexableFields(Columns columns);

    /**
     * Returns the Lucene {@link IndexableField}s resulting from the mapping of the specified {@link Columns}, ignoring
     * any mapping errors.
     *
     * @param columns the columns
     * @return a list of indexable fields
     */
    public List<IndexableField> bestEffortIndexableFields(Columns columns) {
        return bestEffort(columns, this::indexableFields);
    }

    <T> List<IndexableField> bestEffort(T base, Function<T, List<IndexableField>> mapping) {
        try {
            return mapping.apply(base);
        } catch (IndexException e) {
            logger.warn("Error in Lucene index:\n\t" +
                        "while mapping : {}\n\t" +
                        "with mapper   : {}\n\t" +
                        "caused by     : {}", base, this, e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Validates the specified {@link Columns} if {#validated}.
     *
     * @param columns the columns to be validated
     */
    public void validate(Columns columns) {
        if (validated) {
            indexableFields(columns);
        }
    }

    /**
     * Returns the {@link SortField} resulting from the mapping of the specified object.
     *
     * @param name the name of the sorting field
     * @param reverse {@code true} the sort must be reversed, {@code false} otherwise
     * @return the sort field
     */
    public abstract SortField sortField(String name, boolean reverse);

    /**
     * Returns if this maps the specified cell.
     *
     * @param cell the cell name
     * @return {@code true} if this maps the column, {@code false} otherwise
     */
    public boolean mapsCell(String cell) {
        return mappedCells.stream().anyMatch(x -> x.equals(cell));
    }

    void validateTerm(String name, BytesRef term) {
        int maxSize = IndexWriter.MAX_TERM_LENGTH;
        int size = term.length;
        if (size > maxSize) {
            throw new IndexException("Discarding immense term in field='{}', " +
                                     "Lucene only allows terms with at most " +
                                     "{} bytes in length; got {} bytes: {}...",
                                     name, maxSize, size, term.utf8ToString().substring(0, 10));
        }
    }

    protected MoreObjects.ToStringHelper toStringHelper(Object self) {
        return MoreObjects.toStringHelper(self).add("field", field).add("validated", validated);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return toStringHelper(this).toString();
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((analyzer == null) ? 0 : analyzer.hashCode());
        result = prime * result + ((docValues == null) ? 0 : docValues.hashCode());
        result = prime * result + ((excludedTypes == null) ? 0 : excludedTypes.hashCode());
        result = prime * result + ((field == null) ? 0 : field.hashCode());
        result = prime * result + ((mappedCells == null) ? 0 : mappedCells.hashCode());
        result = prime * result + ((mappedColumns == null) ? 0 : mappedColumns.hashCode());
        result = prime * result + ((supportedTypes == null) ? 0 : supportedTypes.hashCode());
        result = prime * result + ((supportsCollections == null) ? 0 : supportsCollections.hashCode());
        result = prime * result + ((validated == null) ? 0 : validated.hashCode());
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Mapper other = (Mapper) obj;
        if (analyzer == null) {
            if (other.analyzer != null)
                return false;
        } else if (!analyzer.equals(other.analyzer))
            return false;
        if (docValues == null) {
            if (other.docValues != null)
                return false;
        } else if (!docValues.equals(other.docValues))
            return false;
        if (excludedTypes == null) {
            if (other.excludedTypes != null)
                return false;
        } else if (!excludedTypes.equals(other.excludedTypes))
            return false;
        if (field == null) {
            if (other.field != null)
                return false;
        } else if (!field.equals(other.field))
            return false;
        if (mappedCells == null) {
            if (other.mappedCells != null)
                return false;
        } else if (!mappedCells.equals(other.mappedCells))
            return false;
        if (mappedColumns == null) {
            if (other.mappedColumns != null)
                return false;
        } else if (!mappedColumns.equals(other.mappedColumns))
            return false;
        if (supportedTypes == null) {
            if (other.supportedTypes != null)
                return false;
        } else if (!supportedTypes.equals(other.supportedTypes))
            return false;
        if (supportsCollections == null) {
            if (other.supportsCollections != null)
                return false;
        } else if (!supportsCollections.equals(other.supportsCollections))
            return false;
        if (validated == null) {
            if (other.validated != null)
                return false;
        } else if (!validated.equals(other.validated))
            return false;
        return true;
    }


}
