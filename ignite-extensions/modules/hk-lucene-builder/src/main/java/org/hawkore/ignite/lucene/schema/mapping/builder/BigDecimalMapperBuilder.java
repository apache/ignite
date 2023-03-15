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
package org.hawkore.ignite.lucene.schema.mapping.builder;

import org.hawkore.ignite.lucene.schema.mapping.BigDecimalMapper;

import com.fasterxml.jackson.annotation.JsonProperty;
 
/**
 * {@link SingleColumnMapperBuilder} to build a new {@link BigDecimalMapper}.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class BigDecimalMapperBuilder extends SingleColumnMapperBuilder<BigDecimalMapper, BigDecimalMapperBuilder> {

    @JsonProperty("integer_digits")
    private Integer integerDigits;

    @JsonProperty("decimal_digits")
    private Integer decimalDigits;

    /**
     * Sets the max number of digits for the integer part.
     *
     * @param integerDigits the max number of digits for the integer part
     * @return this
     */
    public BigDecimalMapperBuilder integerDigits(Integer integerDigits) {
        this.integerDigits = integerDigits;
        return this;
    }

    /**
     * Sets the max number of digits for the decimal part.
     *
     * @param decimalDigits the max number of digits for the decimal part
     * @return this
     */
    public BigDecimalMapperBuilder decimalDigits(Integer decimalDigits) {
        this.decimalDigits = decimalDigits;
        return this;
    }

    /**
     * Returns the {@link BigDecimalMapper} represented by this {@link MapperBuilder}.
     *
     * @param field the name of the field to be built
     * @return the {@link BigDecimalMapper} represented by this
     */
    @Override
    public BigDecimalMapper build(String field) {
        return new BigDecimalMapper(field, column, validated, integerDigits, decimalDigits);
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((decimalDigits == null) ? 0 : decimalDigits.hashCode());
        result = prime * result + ((integerDigits == null) ? 0 : integerDigits.hashCode());
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
        BigDecimalMapperBuilder other = (BigDecimalMapperBuilder) obj;
        if (decimalDigits == null) {
            if (other.decimalDigits != null)
                return false;
        } else if (!decimalDigits.equals(other.decimalDigits))
            return false;
        if (integerDigits == null) {
            if (other.integerDigits != null)
                return false;
        } else if (!integerDigits.equals(other.integerDigits))
            return false;
        return true;
    }
}
