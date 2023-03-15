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
package org.hawkore.ignite.lucene.search.sort;

import static org.apache.lucene.search.SortField.FIELD_SCORE;

import java.util.Collections;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.hawkore.ignite.lucene.IndexException;
import org.hawkore.ignite.lucene.schema.Schema;
import org.hawkore.ignite.lucene.schema.mapping.Mapper;

import com.google.common.base.MoreObjects;

/**
 * @author Eduardo Alonso {@literal <eduardoalonso@stratio.com>}
 */
public class SimpleSortField extends SortField {

    /** The name of field to sortFields by. */
    public final String field;

    /**
     * Returns a new {@link SortField}.
     *
     * @param field the name of field to sort by
     * @param reverse {@code true} if natural order should be reversed, {@code false} otherwise
     */
    public SimpleSortField(String field, Boolean reverse) {
        super(reverse);
        if (field == null || StringUtils.isBlank(field)) {
            throw new IndexException("Field name required");
        }
        this.field = field;
    }

    /**
     * Returns the name of field to sort by.
     *
     * @return the name of field to sort by
     */
    public String getField() {
        return field;
    }

    /**
     * Returns the Lucene {@link org.apache.lucene.search.SortField} representing this {@link SortField}.
     *
     * @param schema the {@link Schema} to be used
     * @return the equivalent Lucene sort field
     */
    @Override
    public org.apache.lucene.search.SortField sortField(Schema schema) {
        if (field.equalsIgnoreCase("score")) {
            return FIELD_SCORE;
        }
        Mapper mapper = schema.mapper(field);
        if (mapper == null) {
            throw new IndexException("No mapper found for sortFields field '{}'", field);
        } else if (!mapper.docValues) {
            throw new IndexException("Field '{}' does not support sorting", field);
        } else {
            return mapper.sortField(field, reverse);
        }
    }

    /** {@inheritDoc} */
    public Set<String> postProcessingFields() {
        return Collections.singleton(field);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("field", field).add("reverse", reverse).toString();
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SimpleSortField otherSimpleSortField = (SimpleSortField) o;
        return reverse == otherSimpleSortField.reverse && field.equals(otherSimpleSortField.field);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        int result = field.hashCode();
        result = 31 * result + (reverse ? 1 : 0);
        return result;
    }
}