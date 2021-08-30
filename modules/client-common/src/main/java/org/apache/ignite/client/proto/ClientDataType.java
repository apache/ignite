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

package org.apache.ignite.client.proto;

/**
 * Client data types.
 */
public class ClientDataType {
    /** Byte. */
    public static final int INT8 = 1;

    /** Short. */
    public static final int INT16 = 2;

    /** Int. */
    public static final int INT32 = 3;

    /** Long. */
    public static final int INT64 = 4;

    /** Float. */
    public static final int FLOAT = 5;

    /** Double. */
    public static final int DOUBLE = 6;

    /** Decimal. */
    public static final int DECIMAL = 7;

    /** UUID. */
    public static final int UUID = 8;

    /** String. */
    public static final int STRING = 9;

    /** Byte array. */
    public static final int BYTES = 10;

    /** BitMask. */
    public static final int BITMASK = 11;

    /** Date. */
    public static final int DATE = 12;

    /** Time. */
    public static final int TIME = 13;

    /** DateTime. */
    public static final int DATETIME = 14;

    /** Timestamp. */
    public static final int TIMESTAMP = 15;

    /** Number. */
    public static final int NUMBER = 16;
}
