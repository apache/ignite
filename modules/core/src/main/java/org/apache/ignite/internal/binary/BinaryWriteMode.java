/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.binary;

/**
 * Various write modes for binary objects.
 */
public enum BinaryWriteMode {
    /** Primitive byte. */
    P_BYTE(GridBinaryMarshaller.BYTE),

    /** Primitive boolean. */
    P_BOOLEAN(GridBinaryMarshaller.BOOLEAN),

    /** Primitive short. */
    P_SHORT(GridBinaryMarshaller.SHORT),

    /** Primitive char. */
    P_CHAR(GridBinaryMarshaller.CHAR),

    /** Primitive int. */
    P_INT(GridBinaryMarshaller.INT),

    /** Primitive long. */
    P_LONG(GridBinaryMarshaller.LONG),

    /** Primitive float. */
    P_FLOAT(GridBinaryMarshaller.FLOAT),

    /** Primitive int. */
    P_DOUBLE(GridBinaryMarshaller.DOUBLE),

    /** */
    BYTE(GridBinaryMarshaller.BYTE),

    /** */
    SHORT(GridBinaryMarshaller.SHORT),

    /** */
    INT(GridBinaryMarshaller.INT),

    /** */
    LONG(GridBinaryMarshaller.LONG),

    /** */
    FLOAT(GridBinaryMarshaller.FLOAT),

    /** */
    DOUBLE(GridBinaryMarshaller.DOUBLE),

    /** */
    CHAR(GridBinaryMarshaller.CHAR),

    /** */
    BOOLEAN(GridBinaryMarshaller.BOOLEAN),

    /** */
    DECIMAL(GridBinaryMarshaller.DECIMAL),

    /** */
    STRING(GridBinaryMarshaller.STRING),

    /** */
    UUID(GridBinaryMarshaller.UUID),

    /** */
    DATE(GridBinaryMarshaller.DATE),

    /** */
    TIMESTAMP(GridBinaryMarshaller.TIMESTAMP),

    /** */
    TIME(GridBinaryMarshaller.TIME),

    /** */
    BYTE_ARR(GridBinaryMarshaller.BYTE_ARR),

    /** */
    SHORT_ARR(GridBinaryMarshaller.SHORT_ARR),

    /** */
    INT_ARR(GridBinaryMarshaller.INT_ARR),

    /** */
    LONG_ARR(GridBinaryMarshaller.LONG_ARR),

    /** */
    FLOAT_ARR(GridBinaryMarshaller.FLOAT_ARR),

    /** */
    DOUBLE_ARR(GridBinaryMarshaller.DOUBLE_ARR),

    /** */
    CHAR_ARR(GridBinaryMarshaller.CHAR_ARR),

    /** */
    BOOLEAN_ARR(GridBinaryMarshaller.BOOLEAN_ARR),

    /** */
    DECIMAL_ARR(GridBinaryMarshaller.DECIMAL_ARR),

    /** */
    STRING_ARR(GridBinaryMarshaller.STRING_ARR),

    /** */
    UUID_ARR(GridBinaryMarshaller.UUID_ARR),

    /** */
    DATE_ARR(GridBinaryMarshaller.DATE_ARR),

    /** */
    TIMESTAMP_ARR(GridBinaryMarshaller.TIMESTAMP_ARR),

    /** */
    TIME_ARR(GridBinaryMarshaller.TIME_ARR),

    /** */
    OBJECT_ARR(GridBinaryMarshaller.OBJ_ARR),

    /** */
    COL(GridBinaryMarshaller.COL),

    /** */
    MAP(GridBinaryMarshaller.MAP),

    /** */
    BINARY_OBJ(GridBinaryMarshaller.OBJ),

    /** */
    ENUM(GridBinaryMarshaller.ENUM),

    /** Binary enum. */
    BINARY_ENUM(GridBinaryMarshaller.ENUM),

    /** */
    ENUM_ARR(GridBinaryMarshaller.ENUM_ARR),

    /** */
    CLASS(GridBinaryMarshaller.CLASS),

    /** */
    PROXY(GridBinaryMarshaller.PROXY),

    /** */
    BINARY(GridBinaryMarshaller.BINARY_OBJ),

    /** */
    OBJECT(GridBinaryMarshaller.OBJ),

    /** */
    OPTIMIZED(GridBinaryMarshaller.OBJ),

    /** */
    EXCLUSION(GridBinaryMarshaller.OBJ);

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
