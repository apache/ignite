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

package org.apache.ignite.internal.portable;

/**
 * Various write modes for binary objects.
 */
public enum BinaryWriteMode {
    /** Primitive byte. */
    P_BYTE(GridPortableMarshaller.BYTE),

    /** Primitive boolean. */
    P_BOOLEAN(GridPortableMarshaller.BOOLEAN),

    /** Primitive short. */
    P_SHORT(GridPortableMarshaller.SHORT),

    /** Primitive char. */
    P_CHAR(GridPortableMarshaller.CHAR),

    /** Primitive int. */
    P_INT(GridPortableMarshaller.INT),

    /** Primitive long. */
    P_LONG(GridPortableMarshaller.LONG),

    /** Primitive float. */
    P_FLOAT(GridPortableMarshaller.FLOAT),

    /** Primitive int. */
    P_DOUBLE(GridPortableMarshaller.DOUBLE),

    /** */
    BYTE(GridPortableMarshaller.BYTE),

    /** */
    SHORT(GridPortableMarshaller.SHORT),

    /** */
    INT(GridPortableMarshaller.INT),

    /** */
    LONG(GridPortableMarshaller.LONG),

    /** */
    FLOAT(GridPortableMarshaller.FLOAT),

    /** */
    DOUBLE(GridPortableMarshaller.DOUBLE),

    /** */
    CHAR(GridPortableMarshaller.CHAR),

    /** */
    BOOLEAN(GridPortableMarshaller.BOOLEAN),

    /** */
    DECIMAL(GridPortableMarshaller.DECIMAL),

    /** */
    STRING(GridPortableMarshaller.STRING),

    /** */
    UUID(GridPortableMarshaller.UUID),

    /** */
    DATE(GridPortableMarshaller.DATE),

    /** */
    TIMESTAMP(GridPortableMarshaller.TIMESTAMP),

    /** */
    BYTE_ARR(GridPortableMarshaller.BYTE_ARR),

    /** */
    SHORT_ARR(GridPortableMarshaller.SHORT_ARR),

    /** */
    INT_ARR(GridPortableMarshaller.INT_ARR),

    /** */
    LONG_ARR(GridPortableMarshaller.LONG_ARR),

    /** */
    FLOAT_ARR(GridPortableMarshaller.FLOAT_ARR),

    /** */
    DOUBLE_ARR(GridPortableMarshaller.DOUBLE_ARR),

    /** */
    CHAR_ARR(GridPortableMarshaller.CHAR_ARR),

    /** */
    BOOLEAN_ARR(GridPortableMarshaller.BOOLEAN_ARR),

    /** */
    DECIMAL_ARR(GridPortableMarshaller.DECIMAL_ARR),

    /** */
    STRING_ARR(GridPortableMarshaller.STRING_ARR),

    /** */
    UUID_ARR(GridPortableMarshaller.UUID_ARR),

    /** */
    DATE_ARR(GridPortableMarshaller.DATE_ARR),

    /** */
    TIMESTAMP_ARR(GridPortableMarshaller.TIMESTAMP_ARR),

    /** */
    OBJECT_ARR(GridPortableMarshaller.OBJ_ARR),

    /** */
    COL(GridPortableMarshaller.COL),

    /** */
    MAP(GridPortableMarshaller.MAP),

    /** */
    MAP_ENTRY(GridPortableMarshaller.MAP_ENTRY),

    /** */
    PORTABLE_OBJ(GridPortableMarshaller.OBJ),

    /** */
    ENUM(GridPortableMarshaller.ENUM),

    /** */
    ENUM_ARR(GridPortableMarshaller.ENUM_ARR),

    /** */
    CLASS(GridPortableMarshaller.CLASS),

    /** */
    PORTABLE(GridPortableMarshaller.PORTABLE_OBJ),

    /** */
    EXTERNALIZABLE(GridPortableMarshaller.OBJ),

    /** */
    OBJECT(GridPortableMarshaller.OBJ),

    /** */
    EXCLUSION(GridPortableMarshaller.OBJ);

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
