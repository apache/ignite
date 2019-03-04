package org.apache.ignite.internal.processors.odbc.odbc;

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

import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * ODBC query execute request with the batch of parameters.
 */
public class OdbcQueryExecuteBatchRequest extends OdbcRequest {
    /** Schema. */
    @GridToStringInclude(sensitive = true)
    private final String schema;

    /** Sql query. */
    @GridToStringInclude(sensitive = true)
    private final String sqlQry;

    /** Last param page flag. */
    private final boolean last;

    /** Sql query arguments. */
    @GridToStringExclude
    private final Object[][] args;

    /** Autocommit flag. */
    @GridToStringInclude
    private final boolean autoCommit;

    /** Query timeout in seconds. */
    @GridToStringInclude
    private final int timeout;

    /**
     * @param schema Schema.
     * @param sqlQry SQL query.
     * @param last Last page flag.
     * @param args Arguments list.
     * @param timeout Timeout in seconds.
     * @param autoCommit Autocommit flag.
     */
    public OdbcQueryExecuteBatchRequest(@Nullable String schema, String sqlQry, boolean last, Object[][] args,
        int timeout, boolean autoCommit) {
        super(QRY_EXEC_BATCH);

        assert sqlQry != null : "SQL query should not be null";
        assert args != null : "Parameters should not be null";

        this.schema = schema;
        this.sqlQry = sqlQry;
        this.last = last;
        this.args = args;
        this.timeout = timeout;
        this.autoCommit = autoCommit;
    }

    /**
     * @return Sql query.
     */
    public String sqlQuery() {
        return sqlQry;
    }

    /**
     * @return Sql query arguments.
     */
    public Object[][] arguments() {
        return args;
    }

    /**
     * @return Schema.
     */
    @Nullable
    public String schema() {
        return schema;
    }

    /**
     * @return Last page flag.
     */
    public boolean last() {
        return last;
    }

    /**
     * @return Timeout in seconds.
     */
    public int timeout() {
        return timeout;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(OdbcQueryExecuteBatchRequest.class, this, "args", args, true);
    }

    /**
     * @return Autocommit flag.
     */
    public boolean autoCommit() {
        return autoCommit;
    }
}
