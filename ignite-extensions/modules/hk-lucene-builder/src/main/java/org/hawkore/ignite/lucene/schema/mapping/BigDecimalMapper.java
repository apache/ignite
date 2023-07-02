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

import java.math.BigDecimal;

import org.apache.commons.lang3.StringUtils;
import org.hawkore.ignite.lucene.IndexException;

/**
 * A {@link Mapper} to map {@link BigDecimal} values. A max number of digits for the integer a decimal parts must be
 * specified.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class BigDecimalMapper extends KeywordMapper {

    /** The default max number of digits for the integer part. */
    public static final int DEFAULT_INTEGER_DIGITS = 32;

    /** The default max number of digits for the decimal part. */
    public static final int DEFAULT_DECIMAL_DIGITS = 32;

    /** THe numeric base. */
    private static final int BASE = 10;

    /** The max number of digits for the integer part. */
    public final int integerDigits;

    /** The max number of digits for the decimal part. */
    public final int decimalDigits;

    private final BigDecimal complement;

    /**
     * Builds a new {@link BigDecimalMapper} using the specified max number of digits for the integer and decimal
     * parts.
     *
     * @param field the name of the field
     * @param column the name of the column to be mapped
     * @param validated if the field must be validated
     * @param integerDigits the max number of digits for the integer part, defaults to {@link #DEFAULT_INTEGER_DIGITS}
     * @param decimalDigits the max number of digits for the decimal part, defaults to {@link #DEFAULT_DECIMAL_DIGITS}
     */
    public BigDecimalMapper(String field,
                            String column,
                            Boolean validated,
                            Integer integerDigits,
                            Integer decimalDigits) {
        super(field, column, validated, NUMERIC_TYPES);

        // Setup integer part mapping
        if (integerDigits != null && integerDigits <= 0) {
            throw new IndexException("Positive integer part digits required");
        }
        this.integerDigits = integerDigits == null ? DEFAULT_INTEGER_DIGITS : integerDigits;

        // Setup decimal part mapping
        if (decimalDigits != null && decimalDigits <= 0) {
            throw new IndexException("Positive decimal part digits required");
        }
        this.decimalDigits = decimalDigits == null ? DEFAULT_DECIMAL_DIGITS : decimalDigits;

        int totalDigits = this.integerDigits + this.decimalDigits;
        BigDecimal divisor = BigDecimal.valueOf(BASE).pow(this.decimalDigits);
        BigDecimal dividend = BigDecimal.valueOf(BASE).pow(totalDigits).subtract(BigDecimal.valueOf(1));
        complement = dividend.divide(divisor);
    }

    /** {@inheritDoc} */
    @Override
    protected String doBase(String name, Object value) {

        // Parse big decimal
        BigDecimal bd;
        try {
            bd = new BigDecimal(value.toString());
        } catch (NumberFormatException e) {
            throw new IndexException("Field '{}' requires a base 10 decimal, but found '{}'", name, value);
        }

        // Split integer and decimal part
        bd = bd.stripTrailingZeros();
        String[] parts = bd.toPlainString().split("\\.");
        validateIntegerPart(name, value, parts);
        validateDecimalPart(name, value, parts);

        BigDecimal complemented = bd.add(complement);
        String bds[] = complemented.toString().split("\\.");
        String integerPart = StringUtils.leftPad(bds[0], integerDigits + 1, '0');
        String decimalPart = bds.length == 2 ? bds[1] : "0";

        return integerPart + "." + decimalPart;
    }

    private void validateIntegerPart(String name, Object value, String[] parts) {
        String integerPart = parts[0];
        if (integerPart.replaceFirst("-", "").length() > integerDigits) {
            throw new IndexException("Field '{}' with value '{}' has more than %d integer digits",
                                     name,
                                     value,
                                     integerDigits);
        }
    }

    private void validateDecimalPart(String name, Object value, String[] parts) {
        String decimalPart = parts.length == 1 ? "0" : parts[1];
        if (decimalPart.length() > decimalDigits) {
            throw new IndexException("Field '{}' with value '{}' has more than %d decimal digits",
                                     name,
                                     value,
                                     decimalDigits);
        }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return toStringHelper(this).add("integerDigits", integerDigits).add("decimalDigits", decimalDigits).toString();
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + decimalDigits;
        result = prime * result + integerDigits;
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
        BigDecimalMapper other = (BigDecimalMapper) obj;
        if (decimalDigits != other.decimalDigits)
            return false;
        if (integerDigits != other.integerDigits)
            return false;
        return true;
    }

    
}
