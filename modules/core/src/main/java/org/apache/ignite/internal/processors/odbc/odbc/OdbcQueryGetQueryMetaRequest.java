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

package org.apache.ignite.internal.processors.odbc.odbc;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * ODBC query get query meta request.
 */
public class OdbcQueryGetQueryMetaRequest extends OdbcRequest {
    /** Schema. */
    protected final String schema;

    /** Query. */
    protected final String query;

    /**
     * @param cmd Command code.
     * @param schema Schema.
     * @param query SQL Query.
     */
    public OdbcQueryGetQueryMetaRequest(byte cmd, String schema, String query) {
        super(cmd);

        this.schema = schema;
        this.query = query;
    }

    /**
     * @return SQL Query.
     */
    public String query() {
        return query;
    }

    /**
     * @return Schema name.
     */
    public String schema() {
        return schema;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(OdbcQueryGetQueryMetaRequest.class, this);
    }
}
