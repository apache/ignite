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

import org.apache.ignite.internal.tostring.S;

/**
 * Temporal native type.
 */
public class TemporalNativeType extends NativeType {
    /**
     * Creates TIME type.
     *
     * @param precision Fractional seconds precision.
     * @return Native type.
     */
    static TemporalNativeType time(int precision) {
        int size = (precision > 3) ? 6 : 4;

        return new TemporalNativeType(NativeTypeSpec.TIME, size, precision);
    }

    /**
     * Creates DATETIME type.
     *
     * @param precision Fractional seconds precision.
     * @return Native type.
     */
    static TemporalNativeType datetime(int precision) {
        int size = NativeTypes.DATE.sizeInBytes() + ((precision > 3) ? 6 : 4);

        return new TemporalNativeType(NativeTypeSpec.DATETIME, size, precision);
    }

    /**
     * Creates TIMESTAMP type.
     *
     * @param precision Fractional seconds precision.
     * @return Native type.
     */
    static TemporalNativeType timestamp(int precision) {
        int size = (precision == 0) ? 8 : 12;

        return new TemporalNativeType(NativeTypeSpec.TIMESTAMP, size, precision);
    }

    /** Fractional seconds precision. */
    private final int precision;

    /**
     * Creates temporal type.
     *
     * @param typeSpec Type spec.
     * @param precision Fractional seconds precision.
     */
    private TemporalNativeType(NativeTypeSpec typeSpec, int size, int precision) {
        super(typeSpec, size);

        if (precision < 0 || precision > 9)
            throw new IllegalArgumentException("Unsupported fractional seconds precision: " + precision);

        this.precision = precision;
    }

    /**
     * Return fractional seconds precision.
     *
     * @return Precision;
     */
    public int precision() {
        return precision;
    }

    /** {@inheritDoc} */
    @Override public boolean mismatch(NativeType type) {
        return super.mismatch(type) || precision < ((TemporalNativeType)type).precision;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TemporalNativeType.class.getSimpleName(), "name", spec(), "precision", precision);
    }
}
