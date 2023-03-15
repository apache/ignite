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
package org.hawkore.ignite.lucene.builder.index.schema.mapping;

import java.math.BigInteger;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A {@link Mapper} to map {@link BigInteger} values. A max number of digits must be specified.
 *
 * @author Andres de la Pena {@literal <adelapena@stratio.com>}
 */
public class BigIntegerMapper extends SingleColumnMapper<BigIntegerMapper> {

    /** The max number of digits. */
    @JsonProperty("digits")
    Integer digits;

    /**
     * Sets the max number of digits.
     *
     * @param digits the max number of digits
     * @return this with the specified max number of digits
     */
    public BigIntegerMapper digits(Integer digits) {
        this.digits = digits;
        return this;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((digits == null) ? 0 : digits.hashCode());
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
        BigIntegerMapper other = (BigIntegerMapper) obj;
        if (digits == null) {
            if (other.digits != null)
                return false;
        } else if (!digits.equals(other.digits))
            return false;
        return true;
    }
    
    
}
