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

package org.apache.ignite.cache.store.jdbc;

import java.io.Serializable;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Description of how field declared in database and in cache.
 */
public class JdbcTypeField implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Field JDBC type in database. */
    private int dbFldType;

    /** Field name in database. */
    private String dbFldName;

    /** Field java type. */
    private Class<?> javaFldType;

    /** Field name in java object. */
    private String javaFldName;

    /**
     * Default constructor.
     */
    public JdbcTypeField() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param dbFldType Field JDBC type in database.
     * @param dbFldName Field name in database.
     * @param javaFldType Field java type.
     * @param javaFldName Field name in java object.
     */
    public JdbcTypeField(int dbFldType, String dbFldName, Class<?> javaFldType, String javaFldName) {
        this.dbFldType = dbFldType;
        this.dbFldName = dbFldName;
        this.javaFldType = javaFldType;
        this.javaFldName = javaFldName;
    }

    /**
     * Copy constructor.
     *
     * @param field Field to copy.
     */
    public JdbcTypeField(JdbcTypeField field) {
        this(field.getDatabaseFieldType(), field.getDatabaseFieldName(),
            field.getJavaFieldType(), field.getJavaFieldName());
    }

    /**
     * @return Column JDBC type in database.
     */
    public int getDatabaseFieldType() {
        return dbFldType;
    }

    /**
     * @param dbFldType Column JDBC type in database.
     * @return {@code this} for chaining.
     */
    public JdbcTypeField setDatabaseFieldType(int dbFldType) {
        this.dbFldType = dbFldType;

        return this;
    }

    /**
     * @return Column name in database.
     */
    public String getDatabaseFieldName() {
        return dbFldName;
    }

    /**
     * @param dbFldName Column name in database.
     * @return {@code this} for chaining.
     */
    public JdbcTypeField setDatabaseFieldName(String dbFldName) {
        this.dbFldName = dbFldName;

        return this;
    }

    /**
     * @return Field java type.
     */
    public Class<?> getJavaFieldType() {
        return javaFldType;
    }

    /**
     * @param javaFldType Corresponding java type.
     * @return {@code this} for chaining.
     */
    public JdbcTypeField setJavaFieldType(Class<?> javaFldType) {
        this.javaFldType = javaFldType;

        return this;
    }

    /**
     * @return Field name in java object.
     */
    public String getJavaFieldName() {
        return javaFldName;
    }

    /**
     * @param javaFldName Field name in java object.
     * @return {@code this} for chaining.
     */
    public JdbcTypeField setJavaFieldName(String javaFldName) {
        this.javaFldName = javaFldName;

        return this;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof JdbcTypeField))
            return false;

        JdbcTypeField that = (JdbcTypeField)o;

        return dbFldType == that.dbFldType && dbFldName.equals(that.dbFldName) &&
            javaFldType == that.javaFldType && javaFldName.equals(that.javaFldName);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = dbFldType;
        res = 31 * res + dbFldName.hashCode();

        res = 31 * res + javaFldType.hashCode();
        res = 31 * res + javaFldName.hashCode();

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(JdbcTypeField.class, this);
    }
}
