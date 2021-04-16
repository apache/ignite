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

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Parse exception.
 */
public class SqlParseException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /** SQL command. */
    private final String sql;

    /** Position. */
    private final int pos;

    /** Error code. */
    private final int code;

    /**
     * Constructor.
     *
     * @param sql SQL command.
     * @param pos Position.
     * @param code Error code (parsing, unsupported operation, etc.).
     * @param msg Message.
     */
    public SqlParseException(String sql, int pos, int code, String msg) {
        super(prepareMessage(sql, pos, msg));

        this.sql = sql;
        this.pos = pos;
        this.code = code;
    }

    /**
     * Copy constructor.
     *
     * @param e Copied exception.
     */
    protected SqlParseException(SqlParseException e) {
        super(e.getMessage());

        sql = e.sql;
        pos = e.pos;
        code = e.code;
    }

    /**
     * Prepare message.
     *
     * @param sql Original SQL.
     * @param pos Position.
     * @param msg Message.
     * @return Prepared message.
     */
    private static String prepareMessage(String sql, int pos, String msg) {
        String sql0;

        if (pos == sql.length())
            sql0 = sql + "[*]";
        else
            sql0 = sql.substring(0, pos) + "[*]" + sql.substring(pos);

        return "Failed to parse SQL statement \"" + sql0 + "\": " + msg;
    }

    /**
     * @return SQL command.
     */
    public String sql() {
        return sql;
    }

    /**
     * @return Position.
     */
    public int position() {
        return pos;
    }

    /**
     * @return Error code.
     */
    public int code() {
        return code;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlParseException.class, this, "msg", getMessage());
    }
}
