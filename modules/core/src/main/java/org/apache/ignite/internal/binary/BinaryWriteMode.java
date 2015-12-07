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

package org.apache.ignite.internal.binary;

/**
 * Various write modes for binary objects.
 */
public enum BinaryWriteMode {
    /** Primitive byte. */
    P_BYTE(InternalBinaryMarshaller.BYTE),

    /** Primitive boolean. */
    P_BOOLEAN(InternalBinaryMarshaller.BOOLEAN),

    /** Primitive short. */
    P_SHORT(InternalBinaryMarshaller.SHORT),

    /** Primitive char. */
    P_CHAR(InternalBinaryMarshaller.CHAR),

    /** Primitive int. */
    P_INT(InternalBinaryMarshaller.INT),

    /** Primitive long. */
    P_LONG(InternalBinaryMarshaller.LONG),

    /** Primitive float. */
    P_FLOAT(InternalBinaryMarshaller.FLOAT),

    /** Primitive int. */
    P_DOUBLE(InternalBinaryMarshaller.DOUBLE),

    /** */
    BYTE(InternalBinaryMarshaller.BYTE),

    /** */
    SHORT(InternalBinaryMarshaller.SHORT),

    /** */
    INT(InternalBinaryMarshaller.INT),

    /** */
    LONG(InternalBinaryMarshaller.LONG),

    /** */
    FLOAT(InternalBinaryMarshaller.FLOAT),

    /** */
    DOUBLE(InternalBinaryMarshaller.DOUBLE),

    /** */
    CHAR(InternalBinaryMarshaller.CHAR),

    /** */
    BOOLEAN(InternalBinaryMarshaller.BOOLEAN),

    /** */
    DECIMAL(InternalBinaryMarshaller.DECIMAL),

    /** */
    STRING(InternalBinaryMarshaller.STRING),

    /** */
    UUID(InternalBinaryMarshaller.UUID),

    /** */
    DATE(InternalBinaryMarshaller.DATE),

    /** */
    TIMESTAMP(InternalBinaryMarshaller.TIMESTAMP),

    /** */
    BYTE_ARR(InternalBinaryMarshaller.BYTE_ARR),

    /** */
    SHORT_ARR(InternalBinaryMarshaller.SHORT_ARR),

    /** */
    INT_ARR(InternalBinaryMarshaller.INT_ARR),

    /** */
    LONG_ARR(InternalBinaryMarshaller.LONG_ARR),

    /** */
    FLOAT_ARR(InternalBinaryMarshaller.FLOAT_ARR),

    /** */
    DOUBLE_ARR(InternalBinaryMarshaller.DOUBLE_ARR),

    /** */
    CHAR_ARR(InternalBinaryMarshaller.CHAR_ARR),

    /** */
    BOOLEAN_ARR(InternalBinaryMarshaller.BOOLEAN_ARR),

    /** */
    DECIMAL_ARR(InternalBinaryMarshaller.DECIMAL_ARR),

    /** */
    STRING_ARR(InternalBinaryMarshaller.STRING_ARR),

    /** */
    UUID_ARR(InternalBinaryMarshaller.UUID_ARR),

    /** */
    DATE_ARR(InternalBinaryMarshaller.DATE_ARR),

    /** */
    TIMESTAMP_ARR(InternalBinaryMarshaller.TIMESTAMP_ARR),

    /** */
    OBJECT_ARR(InternalBinaryMarshaller.OBJ_ARR),

    /** */
    COL(InternalBinaryMarshaller.COL),

    /** */
    MAP(InternalBinaryMarshaller.MAP),

    /** */
    MAP_ENTRY(InternalBinaryMarshaller.MAP_ENTRY),

    /** */
    BINARY_OBJ(InternalBinaryMarshaller.OBJ),

    /** */
    ENUM(InternalBinaryMarshaller.ENUM),

    /** Binary enum. */
    BINARY_ENUM(InternalBinaryMarshaller.ENUM),

    /** */
    ENUM_ARR(InternalBinaryMarshaller.ENUM_ARR),

    /** */
    CLASS(InternalBinaryMarshaller.CLASS),

    /** */
    BINARY(InternalBinaryMarshaller.BINARY_OBJ),

    /** */
    EXTERNALIZABLE(InternalBinaryMarshaller.OBJ),

    /** */
    OBJECT(InternalBinaryMarshaller.OBJ),

    /** */
    EXCLUSION(InternalBinaryMarshaller.OBJ);

    /** Type ID. */
    private final int typeId;

    /**
     * @param typeId Type ID.
     */
    private BinaryWriteMode(int typeId) {
        this.typeId = typeId;
    }

    /**
     * @return Type ID.
     */
    public int typeId() {
        return typeId;
    }
}
