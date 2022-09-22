package de.kp.works.ignite;
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import com.esotericsoftware.kryo.KryoSerializable;
import org.apache.commons.lang3.SerializationUtils;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;

public final class ValueUtils {

    public static ValueType getValueType(Object o) {
        if (o == null) {
            return ValueType.NULL;
        } else if (o instanceof Boolean) {
            return ValueType.BOOLEAN;
        } else if (o instanceof String) {
            return ValueType.STRING;
        } else if (o instanceof Byte) {
            return ValueType.BYTE;
        } else if (o instanceof Short) {
            return ValueType.SHORT;
        } else if (o instanceof Integer) {
            return ValueType.INT;
        } else if (o instanceof Long) {
            return ValueType.LONG;
        } else if (o instanceof Float) {
            return ValueType.FLOAT;
        } else if (o instanceof Double) {
            return ValueType.DOUBLE;
        } else if (o instanceof BigDecimal) {
            return ValueType.DECIMAL;
        } else if (o instanceof LocalDate) {
            return ValueType.DATE;
        } else if (o instanceof LocalTime) {
            return ValueType.TIME;
        } else if (o instanceof LocalDateTime) {
            return ValueType.TIMESTAMP;
        } else if (o instanceof Duration) {
            return ValueType.INTERVAL;
        } else if (o instanceof byte[]) {
            return ValueType.BINARY;
        } else if (o instanceof Enum) {
            return ValueType.ENUM;
        } else if (o instanceof UUID) {
            return ValueType.UUID;
        } else if (o instanceof KryoSerializable) {
            return ValueType.KRYO_SERIALIZABLE;
        } else if (o instanceof Serializable) {
            return ValueType.SERIALIZABLE;
        } else {
            throw new IllegalArgumentException("Unexpected data of type : " + o.getClass().getName());
        }
    }

    public static Object deserialize(byte[] target) {
        if (target == null) return null;
        return SerializationUtils.deserialize(target);
    }

    public static byte[] serialize(Object o) {
        return SerializationUtils.serialize((Serializable) o);
    }

}
