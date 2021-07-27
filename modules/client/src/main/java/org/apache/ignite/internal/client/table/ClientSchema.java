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

package org.apache.ignite.internal.client.table;

import java.util.HashMap;
import java.util.Map;

import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Client schema.
 */
public class ClientSchema {
    /** Schema version. Incremented on each schema modification. */
    private final int ver;

    /** Key columns count. */
    private final int keyColumnCount;

    /** Columns. */
    private final ClientColumn[] columns;

    /** Columns map by name. */
    private final Map<String, ClientColumn> map = new HashMap<>();

    /**
     * Constructor.
     *
     * @param ver Schema version.
     * @param columns Columns.
     */
    public ClientSchema(int ver, ClientColumn[] columns) {
        assert ver >= 0;
        assert columns != null;

        this.ver = ver;
        this.columns = columns;

        var keyCnt = 0;

        for (var col : columns) {
            if (col.key())
                keyCnt++;

            map.put(col.name(), col);
        }

        keyColumnCount = keyCnt;
    }

    /**
     * @return Version.
     */
    public int version() {
        return ver;
    }

    /**
     * @return Columns.
     */
    public @NotNull ClientColumn[] columns() {
        return columns;
    }

    /**
     * Gets a column by name.
     *
     * @param name Column name.
     * @return Column by name.
     * @throws IgniteException When a column with the specified name does not exist.
     */
    public @NotNull ClientColumn column(String name) {
        var column = map.get(name);

        if (column == null)
            throw new IgniteException("Column is not present in schema: " + name);

        return column;
    }

    /**
     * Gets a column by name.
     *
     * @param name Column name.
     * @return Column by name.
     */
    public @Nullable ClientColumn columnSafe(String name) {
        return map.get(name);
    }

    /**
     * @return Key column count.
     */
    public int keyColumnCount() {
        return keyColumnCount;
    }
}
