/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
