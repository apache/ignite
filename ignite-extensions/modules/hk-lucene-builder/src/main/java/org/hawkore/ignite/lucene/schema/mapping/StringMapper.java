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

/**
 * A {@link Mapper} to map a string, not tokenized field.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class StringMapper extends KeywordMapper {

    /** The default case sensitive option. */
    public static final boolean DEFAULT_CASE_SENSITIVE = true;

    /** If it must be case sensitive. */
    public final boolean caseSensitive;

    /**
     * Builds a new {@link StringMapper}.
     *
     * @param field the name of the field
     * @param column the name of the column to be mapped
     * @param validated if the field must be validated
     * @param caseSensitive if the analyzer must be case sensitive
     */
    public StringMapper(String field, String column, Boolean validated, Boolean caseSensitive) {
        super(field, column, validated, PRINTABLE_TYPES);
        this.caseSensitive = caseSensitive == null ? DEFAULT_CASE_SENSITIVE : caseSensitive;
    }

    /** {@inheritDoc} */
    @Override
    protected String doBase(String name, Object value) {
        String string = value.toString();
        return caseSensitive ? string : string.toLowerCase();
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return toStringHelper(this).add("caseSensitive", caseSensitive).toString();
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + (caseSensitive ? 1231 : 1237);
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
        StringMapper other = (StringMapper) obj;
        if (caseSensitive != other.caseSensitive)
            return false;
        return true;
    }
    
    
}
