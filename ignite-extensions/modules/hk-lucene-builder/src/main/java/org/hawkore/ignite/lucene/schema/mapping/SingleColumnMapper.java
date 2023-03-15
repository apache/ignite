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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexableField;
import org.hawkore.ignite.lucene.IndexException;
import org.hawkore.ignite.lucene.column.Column;
import org.hawkore.ignite.lucene.column.Columns;

import com.google.common.base.MoreObjects;

/**
 * Class for mapping between QueryEntity's columns and Lucene documents.
 *
 * @param <T> The base type.
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public abstract class SingleColumnMapper<T extends Comparable<T>> extends Mapper {

    /** The name of the mapped column. */
    public final String column;

    /** The Lucene type for this mapper. */
    public final Class<T> base;

    /**
     * Builds a new {@link SingleColumnMapper} supporting the specified types for indexing and clustering.
     *
     * @param field the name of the field
     * @param column the name of the column to be mapped
     * @param docValues if the mapper supports doc values
     * @param validated if the field must be validated
     * @param analyzer the name of the analyzer to be used
     * @param base the Lucene type for this mapper
     * @param supportedTypes the supported column value data types
     */
    public SingleColumnMapper(String field,
                              String column,
                              Boolean docValues,
                              Boolean validated,
                              String analyzer,
                              Class<T> base,
                              List<Class<?>> supportedTypes) {
        this(field, column, docValues, validated, analyzer, base, supportedTypes, EMPTY_TYPE_LIST);
    }
    /**
     * Builds a new {@link SingleColumnMapper} supporting the specified types for indexing and clustering.
     *
     * @param field the name of the field
     * @param column the name of the column to be mapped
     * @param docValues if the mapper supports doc values
     * @param validated if the field must be validated
     * @param analyzer the name of the analyzer to be used
     * @param base the Lucene type for this mapper
     * @param supportedTypes the supported column value data types
     * @param excludedTypes the explicitly excluded value data types
     */
    public SingleColumnMapper(String field,
                              String column,
                              Boolean docValues,
                              Boolean validated,
                              String analyzer,
                              Class<T> base,
                              List<Class<?>> supportedTypes,
                              List<Class<?>> excludedTypes) {
        super(field,
              docValues,
              validated,
              analyzer,
              Collections.singletonList(column == null ? field : column),
              supportedTypes,
              excludedTypes,
              true);

        if (StringUtils.isWhitespace(column)) {
            throw new IndexException("Column must not be whitespace, but found '{}'", column);
        }

        this.column = column == null ? field : column;
        this.base = base;
    }

    /** {@inheritDoc} */
    @Override
    public List<IndexableField> bestEffortIndexableFields(Columns columns) {
        List<IndexableField> fields = new LinkedList<>();
        columns.foreachWithMapper(column, c -> fields.addAll(bestEffort(c, this::indexableFields)));
        return fields;
    }

    /** {@inheritDoc} */
    @Override
    public List<IndexableField> indexableFields(Columns columns) {
        List<IndexableField> fields = new LinkedList<>();
        columns.foreachWithMapper(column, c -> fields.addAll(indexableFields(c)));
        return fields;
    }

    private List<IndexableField> indexableFields(Column c) {
        String name = column.equals(field) ? c.field() : c.fieldName(field);
        Object value = c.valueOrNull();
        if (value != null) {
            T base = base(c);
            return indexableFields(name, base);
        }
        return Collections.emptyList();
    }

    /**
     * Returns the {@link Field} to search for the mapped column.
     *
     * @param name the name of the column
     * @param value the value of the column
     * @return a list of indexable fields
     */
    public abstract List<IndexableField> indexableFields(String name, T value);

    /**
     * Returns the {@link Column} query value resulting from the mapping of the specified object.
     *
     * @param field the field name
     * @param value the object to be mapped, never is {@code null}
     * @return the {@link Column} index value resulting from the mapping of the specified object
     */
    public final T base(String field, Object value) {
        return value == null ? null : doBase(field, value);
    }

    /**
     * Returns the {@link Column} query value resulting from the mapping of the specified object.
     *
     * @param column the column
     * @return the {@link Column} index value resulting from the mapping of the specified object
     */
    public final T base(Column column) {
        return column == null ? null : column.valueOrNull() == null ? null : doBase(column);
    }

    protected abstract T doBase(@NotNull String field, @NotNull Object value);

    protected final T doBase(Column column) {
        return doBase(column.fieldName(field), column.valueOrNull());
    }

    /** {@inheritDoc} */
    @Override
    protected MoreObjects.ToStringHelper toStringHelper(Object self) {
        return super.toStringHelper(self).add("column", column);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return toStringHelper(this).toString();
    }

    /**
     * {@link SingleColumnMapper} that produces just a single field.
     *
     * @param <T> the base type
     */
    public abstract static class SingleFieldMapper<T extends Comparable<T>> extends SingleColumnMapper<T> {

        /**
         * Builds a new {@link SingleFieldMapper} supporting the specified types for indexing and clustering.
         *
         * @param field the name of the field
         * @param column the name of the column to be mapped
         * @param docValues if the mapper supports doc values
         * @param validated if the field must be validated
         * @param analyzer the name of the analyzer to be used
         * @param base the Lucene type for this mapper
         * @param supportedTypes the supported column value data types
         */
        public SingleFieldMapper(String field,
                                 String column,
                                 Boolean docValues,
                                 Boolean validated,
                                 String analyzer,
                                 Class<T> base,
                                 List<Class<?>> supportedTypes) {
            super(field, column, docValues, validated, analyzer, base, supportedTypes);
        }

        /** {@inheritDoc} */
        @Override
        public List<IndexableField> indexableFields(String name, T value) {
            List<IndexableField> fields = new ArrayList<>(2);
            indexedField(name, value).ifPresent(fields::add);
            sortedField(name, value).ifPresent(fields::add);
            return fields;
        }

        /**
         * Returns the {@link Field} to index by the mapped column.
         *
         * @param name the name of the column
         * @param value the value of the column
         * @return the field to sort by the mapped column
         */
        public abstract Optional<Field> indexedField(String name, T value);

        /**
         * Returns the {@link Field} to sort by the mapped column.
         *
         * @param name the name of the column
         * @param value the value of the column
         * @return the field to sort by the mapped column
         */
        public abstract Optional<Field> sortedField(String name, T value);
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((column == null) ? 0 : column.hashCode());
        return result;
    }
    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        SingleColumnMapper other = (SingleColumnMapper) obj;
        if (column == null) {
            if (other.column != null)
                return false;
        } else if (!column.equals(other.column))
            return false;
        return true;
    }

}
