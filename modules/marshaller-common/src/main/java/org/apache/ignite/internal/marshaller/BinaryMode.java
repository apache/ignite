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

package org.apache.ignite.internal.marshaller;

/**
 * Various read/write modes for binary objects that maps Java types to binary types.
 */
public enum BinaryMode {
    /** Primitive byte. */
    P_BYTE,

    /** Primitive short. */
    P_SHORT,

    /** Primitive int. */
    P_INT,

    /** Primitive long. */
    P_LONG,

    /** Primitive float. */
    P_FLOAT,

    /** Primitive int. */
    P_DOUBLE,

    /** Boxed byte. */
    BYTE,

    /** Boxed short. */
    SHORT,

    /** Boxed int. */
    INT,

    /** Boxed long. */
    LONG,

    /** Boxed float. */
    FLOAT,

    /** Boxed double. */
    DOUBLE,

    /** String. */
    STRING,

    /** Uuid. */
    UUID,

    /** Raw byte array. */
    BYTE_ARR,

    /** BitSet. */
    BITSET,

    /** BigInteger. */
    NUMBER,

    /** BigDecimal. */
    DECIMAL,

    /** Date. */
    DATE,

    /** Time. */
    TIME,

    /** Datetime. */
    DATETIME,

    /** Timestamp. */
    TIMESTAMP;
}
