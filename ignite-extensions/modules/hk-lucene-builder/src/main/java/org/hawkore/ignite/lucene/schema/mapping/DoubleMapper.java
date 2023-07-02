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

import java.util.Optional;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.util.NumericUtils;
import org.hawkore.ignite.lucene.IndexException;

/**
 * A {@link Mapper} to map a double field.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class DoubleMapper extends SingleColumnMapper.SingleFieldMapper<Double> {
	
    /** The default boost. */
    public static final float DEFAULT_BOOST = 1.0f;

    /** The boost. */
    public final Float boost;

    /**
     * Builds a new {@link DoubleMapper} using the specified boost.
     *
     * @param field the name of the field
     * @param column the name of the column to be mapped
     * @param validated if the field must be validated
     * @param boost the boost
     */
    public DoubleMapper(String field, String column, Boolean validated, Float boost) {
        super(field, column, true, validated, null, Double.class, NUMERIC_TYPES);
        this.boost = boost == null ? DEFAULT_BOOST : boost;
    }

    /** {@inheritDoc} */
    @Override
    protected Double doBase(String name, Object value) {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        } else if (value instanceof String) {
            try {
                return Double.valueOf((String) value);
            } catch (NumberFormatException e) {
                throw new IndexException("Field '{}' with value '{}' can not be parsed as double", name, value);
            }
        }
        throw new IndexException("Field '{}' requires a double, but found '{}'", name, value);
    }

    /** {@inheritDoc} */
    @Override
    public Optional<Field> indexedField(String name, Double value) {
        DoublePoint field = new DoublePoint(name, value);
		return Optional.of(field);
    }

    /** {@inheritDoc} */
    @Override
    public Optional<Field> sortedField(String name, Double value) {
        long sortable = NumericUtils.doubleToSortableLong(value);
        return Optional.of(new SortedNumericDocValuesField(name, sortable));
    }

    /** {@inheritDoc} */
    @Override
    public SortField sortField(String name, boolean reverse) {
        return new SortedNumericSortField(name, Type.DOUBLE, reverse);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return toStringHelper(this).add("boost", boost).toString();
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((boost == null) ? 0 : boost.hashCode());
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
        DoubleMapper other = (DoubleMapper) obj;
        if (boost == null) {
            if (other.boost != null)
                return false;
        } else if (!boost.equals(other.boost))
            return false;
        return true;
    }
    
    
}
