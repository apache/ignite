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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.util.ObjectFactory;

/**
 * Marshaller utility class.
 */
public final class MarshallerUtil {
    /**
     * Calculates size for serialized value of varlen type.
     *
     * @param val Field value.
     * @param type Mapped type.
     * @return Serialized value size.
     * @throws InvalidTypeException If type is unsupported.
     */
    public static int getValueSize(Object val, NativeType type) throws InvalidTypeException {
        switch (type.spec()) {
            case BYTES:
                return ((byte[])val).length;

            case STRING:
                // Overestimating size here prevents from later unwanted row buffer expanding.
                return ((CharSequence)val).length() << 1;

            case NUMBER:
                return RowAssembler.sizeInBytes((BigInteger)val);

            case DECIMAL:
                return RowAssembler.sizeInBytes((BigDecimal)val);

            default:
                throw new InvalidTypeException("Unsupported variable-length type: " + type);
        }
    }

    /**
     * Gets binary read/write mode for given class.
     *
     * @param cls Type.
     * @return Binary mode.
     */
    public static BinaryMode mode(Class<?> cls) {
        assert cls != null;

        // Primitives.
        if (cls == byte.class)
            return BinaryMode.P_BYTE;
        else if (cls == short.class)
            return BinaryMode.P_SHORT;
        else if (cls == int.class)
            return BinaryMode.P_INT;
        else if (cls == long.class)
            return BinaryMode.P_LONG;
        else if (cls == float.class)
            return BinaryMode.P_FLOAT;
        else if (cls == double.class)
            return BinaryMode.P_DOUBLE;

            // Boxed primitives.
        else if (cls == Byte.class)
            return BinaryMode.BYTE;
        else if (cls == Short.class)
            return BinaryMode.SHORT;
        else if (cls == Integer.class)
            return BinaryMode.INT;
        else if (cls == Long.class)
            return BinaryMode.LONG;
        else if (cls == Float.class)
            return BinaryMode.FLOAT;
        else if (cls == Double.class)
            return BinaryMode.DOUBLE;

            // Other types
        else if (cls == byte[].class)
            return BinaryMode.BYTE_ARR;
        else if (cls == String.class)
            return BinaryMode.STRING;
        else if (cls == UUID.class)
            return BinaryMode.UUID;
        else if (cls == BitSet.class)
            return BinaryMode.BITSET;
        else if (cls == BigInteger.class)
            return BinaryMode.NUMBER;
        else if (cls == BigDecimal.class)
            return BinaryMode.DECIMAL;

        return null;
    }

    /**
     * Creates object factory for class.
     *
     * @param tCls Target class.
     * @param <T> Target type.
     * @return Object factory.
     */
    public static <T> ObjectFactory<T> factoryForClass(Class<T> tCls) {
        if (mode(tCls) == null)
            return new ObjectFactory<>(tCls);
        else
            return null;
    }

    /** Stub. */
    private MarshallerUtil() {
    }
}
