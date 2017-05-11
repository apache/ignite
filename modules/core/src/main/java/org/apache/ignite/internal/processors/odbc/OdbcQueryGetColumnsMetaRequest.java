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

import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * ODBC query get columns meta request.
 */
public class OdbcQueryGetColumnsMetaRequest extends SqlListenerRequest {
    /** Cache name. */
    private final String cacheName;

    /** Table name. */
    private final String tableName;

    /** Column name. */
    private final String columnName;

    /**
     * @param cacheName Cache name.
     * @param tableName Table name.
     * @param columnName Column name.
     */
    public OdbcQueryGetColumnsMetaRequest(String cacheName, String tableName, String columnName) {
        super(META_COLS);

        this.cacheName = cacheName;
        this.tableName = tableName;
        this.columnName = columnName;
    }

    /**
     * @return Cache name.
     */
    @Nullable public String cacheName() {
        return cacheName;
    }

    /**
     * @return Table name.
     */
    public String tableName() {
        return tableName;
    }

    /**
     * @return Column name.
     */
    public String columnName() {
        return columnName;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(OdbcQueryGetColumnsMetaRequest.class, this);
    }
}