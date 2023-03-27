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

package org.apache.ignite.internal.sql;

import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;

/**
 * Parse exception guarantees parse error without. Such error deliver to user
 * statement isn't passed to H2 parser.
 */
public class SqlStrictParseException extends SqlParseException {
    /** */
    private static final long serialVersionUID = 0L;

    /** SQL error code. */
    private final int errCode;

    /**
     * Constructor.
     * @param e SQL parse exception.
     */
    public SqlStrictParseException(SqlParseException e) {
        this(e.getMessage(), IgniteQueryErrorCode.PARSING, e);
    }

    /**
     * Constructor.
     * @param e SQL parse exception.
     * @param msg Error message.
     * @param errCode SQL error code.
     */
    public SqlStrictParseException(String msg, int errCode, SqlParseException e) {
        super(e.sql(), e.position(), e.code(), msg);

        this.errCode = errCode;
    }

    /**
     * @return SQL error code.
     */
    public int errorCode() {
        return errCode;
    }
}
