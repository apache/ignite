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

package org.apache.ignite.internal.processors.query.schema;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Schema operation exception.
 */
public class SchemaOperationException extends IgniteCheckedException {
    /** */
    private static final long serialVersionUID = 0L;

    /** Code: generic error. */
    public static final int CODE_GENERIC = 0;

    /** Code: cache not found. */
    public static final int CODE_CACHE_NOT_FOUND = 1;

    /** Code: table not found. */
    public static final int CODE_TABLE_NOT_FOUND = 2;

    /** Code: table already exists. */
    public static final int CODE_TABLE_EXISTS = 3;

    /** Code: column not found. */
    public static final int CODE_COLUMN_NOT_FOUND = 4;

    /** Code: column already exists. */
    public static final int CODE_COLUMN_EXISTS = 5;

    /** Code: index not found. */
    public static final int CODE_INDEX_NOT_FOUND = 6;

    /** Code: index already exists. */
    public static final int CODE_INDEX_EXISTS = 7;

    /** Code: cache already indexed. */
    public static final int CODE_CACHE_ALREADY_INDEXED = 8;

    /** Error code. */
    private final int code;

    /**
     * Constructor for specific error type.
     *
     * @param code Code.
     * @param objName Object name.
     */
    public SchemaOperationException(int code, String objName) {
        super(message(code, objName));

        this.code = code;
    }

    /**
     * Constructor for generic error.
     *
     * @param msg Message.
     */
    public SchemaOperationException(String msg) {
        this(msg, null);
    }

    /**
     * Constructor for generic error.
     *
     * @param msg Message.
     * @param cause Cause.
     */
    public SchemaOperationException(String msg, Throwable cause) {
        super(msg, cause);

        code = CODE_GENERIC;
    }

    /**
     * @return Code.
     */
    public int code() {
        return code;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SchemaOperationException.class, this, "msg", getMessage());
    }

    /**
     * Create message for specific code and object name.
     *
     * @param code Code.
     * @param objName Object name.
     * @return Message.
     */
    private static String message(int code, String objName) {
        switch (code) {
            case CODE_CACHE_NOT_FOUND:
                return "Cache doesn't exist: " + objName;

            case CODE_TABLE_NOT_FOUND:
                return "Table doesn't exist: " + objName;

            case CODE_TABLE_EXISTS:
                return "Table already exists: " + objName;

            case CODE_COLUMN_NOT_FOUND:
                return "Column doesn't exist: " + objName;

            case CODE_COLUMN_EXISTS:
                return "Column already exists: " + objName;

            case CODE_INDEX_NOT_FOUND:
                return "Index doesn't exist: " + objName;

            case CODE_INDEX_EXISTS:
                return "Index already exists: " + objName;

            case CODE_CACHE_ALREADY_INDEXED:
                return "Cache is already indexed: " + objName;

            default:
                assert false;

                return null;
        }
    }
}
