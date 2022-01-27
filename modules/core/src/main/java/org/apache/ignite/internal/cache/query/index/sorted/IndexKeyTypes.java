/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.cache.query.index.sorted;

/**
 * List of available types to use as index key.
 */
public class IndexKeyTypes {
    /** The data type is unknown at this time. */
    public static final int UNKNOWN = -1;

    /** The value type for NULL. */
    public static final int NULL = 0;

    /** The value type for BOOLEAN values. */
    public static final int BOOLEAN = 1;

    /** The value type for BYTE values. */
    public static final int BYTE = 2;

    /** The value type for SHORT values. */
    public static final int SHORT = 3;

    /** The value type for INT values. */
    public static final int INT = 4;

    /** The value type for INT values. */
    public static final int LONG = 5;

    /** The value type for DECIMAL values. */
    public static final int DECIMAL = 6;

    /** The value type for DOUBLE values. */
    public static final int DOUBLE = 7;

    /** The value type for FLOAT values. */
    public static final int FLOAT = 8;

    /** The value type for TIME values. */
    public static final int TIME = 9;

    /**
     * The value type for DATE values.
     */
    public static final int DATE = 10;

    /**
     * The value type for TIMESTAMP values.
     */
    public static final int TIMESTAMP = 11;

    /**
     * The value type for BYTES values.
     */
    public static final int BYTES = 12;

    /**
     * The value type for STRING values.
     */
    public static final int STRING = 13;

    /**
     * The value type for case insensitive STRING values.
     */
    public static final int STRING_IGNORECASE = 14;

    /**
     * The value type for BLOB values.
     */
    public static final int BLOB = 15;

    /**
     * The value type for CLOB values.
     */
    public static final int CLOB = 16;

    /**
     * The value type for ARRAY values.
     */
    public static final int ARRAY = 17;

    /**
     * The value type for RESULT_SET values.
     */
    public static final int RESULT_SET = 18;

    /**
     * The value type for JAVA_OBJECT values.
     */
    public static final int JAVA_OBJECT = 19;

    /**
     * The value type for UUID values.
     */
    public static final int UUID = 20;

    /**
     * The value type for string values with a fixed size.
     */
    public static final int STRING_FIXED = 21;

    /**
     * The value type for string values with a fixed size.
     */
    public static final int GEOMETRY = 22;

    // 23 was a short-lived experiment "TIMESTAMP UTC" which has been removed.

    /**
     * The value type for TIMESTAMP WITH TIME ZONE values.
     */
    public static final int TIMESTAMP_TZ = 24;

    /**
     * The value type for ENUM values.
     */
    public static final int ENUM = 25;
}
