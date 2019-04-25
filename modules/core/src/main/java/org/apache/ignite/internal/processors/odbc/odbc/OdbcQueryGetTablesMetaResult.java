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

import java.util.Collection;

/**
 * Query get columns meta result.
 */
public class OdbcQueryGetTablesMetaResult {
    /** Query result rows. */
    private final Collection<OdbcTableMeta> meta;

    /**
     * @param meta Column metadata.
     */
    public OdbcQueryGetTablesMetaResult(Collection<OdbcTableMeta> meta) {
        this.meta = meta;
    }

    /**
     * @return Query result rows.
     */
    public Collection<OdbcTableMeta> meta() {
        return meta;
    }
}
