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

package org.apache.ignite.schema.parser;

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

    /**
     * @param name Column name.
     * @param type Column JDBC type.
     * @param key {@code true} if this column belongs to primary key.
     * @param nullable {@code true} if {@code NULL } allowed for column in database.
     */
    public DbColumn(String name, int type, boolean key, boolean nullable) {
        this.name = name;
        this.type = type;
        this.key = key;
        this.nullable = nullable;
    }

    /**
     * @return Column name.
     */
    public String name() {
        return name;
    }

    /**
     * @return Column JDBC type.
     */
    public int type() {
        return type;
    }

    /**
     * @return {@code true} if this column belongs to primary key.
     */
    public boolean key() {
        return key;
    }

    /**
     * @return nullable {@code true} if {@code NULL } allowed for column in database.
     */
    public boolean nullable() {
        return nullable;
    }
}
