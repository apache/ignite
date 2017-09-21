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

package org.apache.ignite.internal.processors.odbc;

import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * ODBC query execute request.
 */
public class OdbcQueryExecuteRequest extends OdbcRequest {
    /** Cache name. */
    private final String cacheName;

    /** Sql query. */
    @GridToStringInclude(sensitive = true)
    private final String sqlQry;

    /** Sql query arguments. */
    @GridToStringExclude
    private final Object[] args;

    /**
     * @param cacheName Cache name.
     * @param sqlQry SQL query.
     * @param args Arguments list.
     */
    public OdbcQueryExecuteRequest(String cacheName, String sqlQry, Object[] args) {
        super(EXECUTE_SQL_QUERY);

        this.cacheName = cacheName.isEmpty() ? null : cacheName;
        this.sqlQry = sqlQry;
        this.args = args;
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
    public Object[] arguments() {
        return args;
    }

    /**
     * @return Cache name.
     */
    @Nullable public String cacheName() {
        return cacheName;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(OdbcQueryExecuteRequest.class, this, "args", args, true);
    }
}
