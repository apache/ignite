/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.odbc.odbc;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * ODBC query get columns meta request.
 */
public class OdbcQueryGetColumnsMetaRequest extends OdbcRequest {
    /** Schema pattern. */
    private final String schemaPattern;

    /** Table pattern. */
    private final String tablePattern;

    /** Column pattern. */
    private final String columnPattern;

    /**
     * @param schemaPattern Schema pattern.
     * @param tablePattern Table pattern.
     * @param columnPattern Column pattern.
     */
    public OdbcQueryGetColumnsMetaRequest(String schemaPattern, String tablePattern, String columnPattern) {
        super(META_COLS);

        this.schemaPattern = schemaPattern;
        this.tablePattern = tablePattern;
        this.columnPattern = columnPattern;
    }

    /**
     * @return Schema pattern.
     */
    @Nullable public String schemaPattern() {
        return schemaPattern;
    }

    /**
     * @return Table pattern.
     */
    public String tablePattern() {
        return tablePattern;
    }

    /**
     * @return Column pattern.
     */
    public String columnPattern() {
        return columnPattern;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(OdbcQueryGetColumnsMetaRequest.class, this);
    }
}