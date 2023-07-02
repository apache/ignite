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

import java.util.List;
import java.util.Optional;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.util.BytesRef;

/**
 * A {@link Mapper} to map a string, not tokenized field.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public abstract class KeywordMapper extends SingleColumnMapper.SingleFieldMapper<String> {

    static final FieldType FIELD_TYPE = new FieldType();

    static {
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        FIELD_TYPE.setTokenized(true);
        FIELD_TYPE.freeze();
    }

    /**
     * Builds  a new {@link KeywordMapper}.
     *
     * @param field the name of the field
     * @param column the name of the column to be mapped
     * @param validated if the field must be validated
     * @param supportedTypes the supported QueryEntity types
     */
    KeywordMapper(String field, String column, Boolean validated, List<Class<?>> supportedTypes) {
        super(field, column, true, validated, KEYWORD_ANALYZER, String.class, supportedTypes);
    }

    /** {@inheritDoc} */
    @Override
    public Optional<Field> indexedField(String name, String value) {
        validateTerm(name, new BytesRef(value));
        return Optional.of(new Field(name, value, FIELD_TYPE));
    }

    /** {@inheritDoc} */
    @Override
    public Optional<Field> sortedField(String name, String value) {
        BytesRef bytes = new BytesRef(value);
        validateTerm(name, bytes);
        return Optional.of(new SortedSetDocValuesField(name, bytes));
    }

    /** {@inheritDoc} */
    @Override
    public final SortField sortField(String name, boolean reverse) {
        return new SortedSetSortField(name, reverse);
    }
}
