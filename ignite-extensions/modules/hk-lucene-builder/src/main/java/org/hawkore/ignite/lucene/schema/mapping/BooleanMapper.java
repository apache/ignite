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

import java.util.Arrays;

import org.hawkore.ignite.lucene.IndexException;

/**
 * A {@link Mapper} to map a boolean field.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class BooleanMapper extends KeywordMapper {

    /** The {@code String} representation of a true value. */
    private static final String TRUE = "true";

    /** The {@code String} representation of a false value. */
    private static final String FALSE = "false";

    /**
     * Builds a new {@link BooleanMapper}.
     *
     * @param field the name of the field
     * @param column the name of the column to be mapped
     * @param validated if the field must be validated
     */
    public BooleanMapper(String field, String column, Boolean validated) {
        super(field, column, validated, Arrays.asList(String.class, Boolean.class));
    }

    /** {@inheritDoc} */
    @Override
    protected String doBase(String name, Object value) {
        if (value instanceof Boolean) {
            return (Boolean) value ? TRUE : FALSE;
        } else if (value instanceof String) {
            return base(name, (String) value);
        }
        throw new IndexException("Field '{}' requires a boolean, but found '{}'", name, value);
    }

    private String base(String name, String value) {
        if (value.equalsIgnoreCase(TRUE)) {
            return TRUE;
        } else if (value.equalsIgnoreCase(FALSE)) {
            return FALSE;
        } else {
            throw new IndexException("Boolean field '{}' requires either '{}' or '{}', but found '{}'",
                                     name, TRUE, FALSE, value);
        }
    }

}
