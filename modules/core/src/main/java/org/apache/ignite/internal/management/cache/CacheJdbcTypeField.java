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

package org.apache.ignite.internal.management.cache;

import org.apache.ignite.cache.store.jdbc.JdbcTypeField;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Data transfer object for {@link JdbcTypeField}.
 */
public class CacheJdbcTypeField extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Column name in database. */
    @Order(value = 0)
    String dbName;

    /** Column JDBC type in database. */
    @Order(value = 1)
    int dbType;

    /** Field name in java object. */
    @Order(value = 2)
    String javaName;

    /** Corresponding java type. */
    @Order(value = 3)
    String javaType;

    /**
     * Empty constructor.
     */
    public CacheJdbcTypeField() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param dbName Column name in database.
     * @param dbType Column JDBC type in database.
     * @param javaName Field name in java object.
     * @param javaType Corresponding java type.
     */
    public CacheJdbcTypeField(String dbName, int dbType, String javaName, String javaType) {
        this.dbName = dbName;
        this.dbType = dbType;
        this.javaName = javaName;
        this.javaType = javaType;
    }

    /**
     * @return Column name in database.
     */
    public String getDatabaseName() {
        return dbName;
    }

    /**
     * @return Column JDBC type in database.
     */
    public int getDatabaseType() {
        return dbType;
    }

    /**
     * @return Field name in java object.
     */
    public String getJavaName() {
        return javaName;
    }

    /**
     * @return Corresponding java type.
     */
    public String getJavaType() {
        return javaType;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheJdbcTypeField.class, this);
    }
}
