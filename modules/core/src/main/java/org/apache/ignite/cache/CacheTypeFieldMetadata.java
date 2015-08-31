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

package org.apache.ignite.cache;

import java.io.Serializable;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Type field metadata.
 */
public class CacheTypeFieldMetadata implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Column name in database. */
    private String dbName;

    /** Column JDBC type in database. */
    private int dbType;

    /** Field name in java object. */
    private String javaName;

    /** Corresponding java type. */
    private Class<?> javaType;

    /**
     * Default constructor.
     */
    public CacheTypeFieldMetadata() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param dbName Column name in database.
     * @param dbType Column JDBC type in database.
     * @param javaName Field name in java object.
     * @param javaType Field java type.
     */
    public CacheTypeFieldMetadata(String dbName, int dbType, String javaName, Class<?> javaType) {
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
     * @param dbName Column name in database.
     */
    public void setDatabaseName(String dbName) {
        this.dbName = dbName;
    }

    /**
     * @return Column JDBC type in database.
     */
    public int getDatabaseType() {
        return dbType;
    }

    /**
     * @param dbType Column JDBC type in database.
     */
    public void setDatabaseType(int dbType) {
        this.dbType = dbType;
    }

    /**
     * @return Field name in java object.
     */
    public String getJavaName() {
        return javaName;
    }

    /**
     * @param javaName Field name in java object.
     */
    public void setJavaName(String javaName) {
        this.javaName = javaName;
    }

    /**
     * @return Field java type.
     */
    public Class<?> getJavaType() {
        return javaType;
    }

    /**
     * @param javaType Corresponding java type.
     */
    public void setJavaType(Class<?> javaType) {
        this.javaType = javaType;
    }

     /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof CacheTypeFieldMetadata))
            return false;

        CacheTypeFieldMetadata that = (CacheTypeFieldMetadata)o;

        return javaName.equals(that.javaName) && dbName.equals(that.dbName) &&
            javaType == that.javaType && dbType == that.dbType;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = dbName.hashCode();

        res = 31 * res + dbType;
        res = 31 * res + javaName.hashCode();
        res = 31 * res + javaType.hashCode();

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheTypeFieldMetadata.class, this);
    }
}