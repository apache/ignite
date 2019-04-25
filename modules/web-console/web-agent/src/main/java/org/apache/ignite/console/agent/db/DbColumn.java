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

package org.apache.ignite.console.agent.db;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Database table column.
 */
public class DbColumn {
    /** Column name. */
    private final String name;

    /** Column JDBC type. */
    private final int type;

    /** Is this column belongs to primary key. */
    private final boolean key;

    /** Is {@code NULL} allowed for column in database. */
    private final boolean nullable;

    /** Whether column unsigned. */
    private final boolean unsigned;

    /**
     * @param name Column name.
     * @param type Column JDBC type.
     * @param key {@code true} if this column belongs to primary key.
     * @param nullable {@code true} if {@code NULL } allowed for column in database.
     * @param unsigned {@code true} if column is unsigned.
     */
    public DbColumn(String name, int type, boolean key, boolean nullable, boolean unsigned) {
        this.name = name;
        this.type = type;
        this.key = key;
        this.nullable = nullable;
        this.unsigned = unsigned;
    }

    /**
     * @return Column name.
     */
    public String getName() {
        return name;
    }

    /**
     * @return Column JDBC type.
     */
    public int getType() {
        return type;
    }

    /**
     * @return {@code true} if this column belongs to primary key.
     */
    public boolean isKey() {
        return key;
    }

    /**
     * @return {@code true} if {@code NULL } allowed for column in database.
     */
    public boolean isNullable() {
        return nullable;
    }

    /**
     * @return {@code true} if column is unsigned.
     */
    public boolean isUnsigned() {
        return unsigned;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DbColumn.class, this);
    }
}
