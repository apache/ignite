/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.schema;

import java.util.Objects;
import org.apache.ignite.internal.tostring.S;

/**
 * Decimal column type.
 */
public class DecimalNativeType extends NativeType {
    /** Precision. */
    private final int precision;

    /** Scale. */
    private final int scale;

    /**
     * The constructor.
     *
     * @param precision Precision.
     * @param scale Scale.
     */
    DecimalNativeType(int precision, int scale) {
        super(NativeTypeSpec.DECIMAL);

        this.precision = precision;
        this.scale = scale;
    }

    /**
     * @return Precision.
     */
    public int precision() {
        return precision;
    }

    /**
     * @return Scale.
     */
    public int scale() {
        return scale;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        if (!super.equals(o))
            return false;

        DecimalNativeType type = (DecimalNativeType)o;

        return precision == type.precision &&
            scale == type.scale;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(super.hashCode(), precision, scale);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DecimalNativeType.class.getSimpleName(), "name", spec(), "precision", precision, "scale", scale);
    }
}
