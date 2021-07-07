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

package org.apache.ignite.internal.schema.marshaller;

import org.apache.ignite.internal.schema.NativeTypeSpec;

/**
 * Various read/write modes for binary objects that maps Java types to binary types.
 */
public enum BinaryMode {
    /** Primitive byte. */
    P_BYTE(NativeTypeSpec.INT8),

    /** Primitive short. */
    P_SHORT(NativeTypeSpec.INT16),

    /** Primitive int. */
    P_INT(NativeTypeSpec.INT32),

    /** Primitive long. */
    P_LONG(NativeTypeSpec.INT64),

    /** Primitive float. */
    P_FLOAT(NativeTypeSpec.FLOAT),

    /** Primitive int. */
    P_DOUBLE(NativeTypeSpec.DOUBLE),

    /** Boxed byte. */
    BYTE(NativeTypeSpec.INT8),

    /** Boxed short. */
    SHORT(NativeTypeSpec.INT16),

    /** Boxed int. */
    INT(NativeTypeSpec.INT32),

    /** Boxed long. */
    LONG(NativeTypeSpec.INT64),

    /** Boxed float. */
    FLOAT(NativeTypeSpec.FLOAT),

    /** Boxed double. */
    DOUBLE(NativeTypeSpec.DOUBLE),

    /** String. */
    STRING(NativeTypeSpec.STRING),

    /** Uuid. */
    UUID(NativeTypeSpec.UUID),

    /** Raw byte array. */
    BYTE_ARR(NativeTypeSpec.BYTES),

    /** BitSet.*/
    BITSET(NativeTypeSpec.BITMASK);

    /** Natove type spec. */
    private final NativeTypeSpec typeSpec;

    /**
     * @param typeSpec Native type spec.
     */
    BinaryMode(NativeTypeSpec typeSpec) {
        this.typeSpec = typeSpec;
    }

    /**
     * @return Native type spec.
     */
    public NativeTypeSpec typeSpec() {
        return typeSpec;
    }
}
