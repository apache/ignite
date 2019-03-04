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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.internal.processors.cache.QueryCursorImpl;
import org.apache.ignite.internal.processors.odbc.ClientListenerProtocolVersion;
import org.apache.ignite.internal.processors.query.GridQueryFieldMetadata;

/**
 * Represents single result set.
 */
public class OdbcResultSet {
    /** Cursor. */
    private final QueryCursorImpl<List<?>> cursor;

    /** Current iterator. */
    private Iterator iter;

    /** Client version. */
    private ClientListenerProtocolVersion ver;

    /**
     * Constructor.
     * @param cursor Result set cursor.
     * @param ver Client version.
     */
    OdbcResultSet(FieldsQueryCursor<List<?>> cursor, ClientListenerProtocolVersion ver) {
        assert cursor instanceof QueryCursorImpl;

        this.cursor = (QueryCursorImpl<List<?>>)cursor;
        this.ver = ver;

        if (this.cursor.isQuery())
            iter = this.cursor.iterator();
        else
            iter = null;
    }

    /**
     * @return {@code true} if has non-fetched rows.
     */
    public boolean hasUnfetchedRows() {
        return iter != null && iter.hasNext();
    }

    /**
     * @return Fields metadata of the current result set.
     */
    public Collection<OdbcColumnMeta> fieldsMeta() {
        if (!cursor.isQuery())
            return new ArrayList<>();

        return convertMetadata(cursor.fieldsMeta(), ver);
    }

    /**
     * Fetch up to specified number of rows of result set.
     * @param maxSize Maximum number of records to fetch.
     * @return List of fetched records.
     */
    public List<Object> fetch(int maxSize) {
        List<Object> items = new ArrayList<>(maxSize);

        if (iter == null)
            return items;

        for (int i = 0; i < maxSize && iter.hasNext(); ++i)
            items.add(iter.next());

        return items;
    }

    /**
     * Convert metadata in collection from {@link GridQueryFieldMetadata} to
     * {@link OdbcColumnMeta}.
     *
     * @param meta Internal query field metadata.
     * @param ver Client version.
     * @return Odbc query field metadata.
     */
    private static Collection<OdbcColumnMeta> convertMetadata(Collection<GridQueryFieldMetadata> meta,
        ClientListenerProtocolVersion ver) {
        List<OdbcColumnMeta> res = new ArrayList<>();

        if (meta != null) {
            for (GridQueryFieldMetadata info : meta)
                res.add(new OdbcColumnMeta(info, ver));
        }

        return res;
    }
}
