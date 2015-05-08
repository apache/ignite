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

package org.apache.ignite.internal.visor.cache;

import org.apache.ignite.cache.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;

/**
 * Data transfer object for {@link CacheTypeFieldMetadata}.
 */
public class VisorCacheTypeFieldMetadata implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Column name in database. */
    private String dbName;

    /** Column JDBC type in database. */
    private int dbType;

    /** Field name in java object. */
    private String javaName;

    /** Corresponding java type. */
    private String javaType;

    /**
     * @param f Actual field metadata.
     * @return Data transfer object for given cache field metadata.
     */
    public static VisorCacheTypeFieldMetadata from(CacheTypeFieldMetadata f) {
        VisorCacheTypeFieldMetadata fieldMetadata = new VisorCacheTypeFieldMetadata();

        fieldMetadata.dbName(f.getDatabaseName());
        fieldMetadata.dbType(f.getDatabaseType());
        fieldMetadata.javaName(f.getJavaName());
        fieldMetadata.javaType(U.compact(f.getJavaType().getName()));

        return fieldMetadata;
    }

    /**
     * @param dbName New column name in database.
     */
    public void dbName(String dbName) {
        this.dbName = dbName;
    }

    /**
     * @return Column name in database.
     */
    public String dbName() {
        return dbName;
    }

    /**
     * @param dbType New column JDBC type in database.
     */
    public void dbType(int dbType) {
        this.dbType = dbType;
    }

    /**
     * @return Column JDBC type in database.
     */
    public int dbType() {
        return dbType;
    }

    /**
     * @param javaName New field name in java object.
     */
    public void javaName(String javaName) {
        this.javaName = javaName;
    }

    /**
     * @return Field name in java object.
     */
    public String javaName() {
        return javaName;
    }

    /**
     * @param javaType New corresponding java type.
     */
    public void javaType(String javaType) {
        this.javaType = javaType;
    }

    /**
     * @return Corresponding java type.
     */
    public String javaType() {
        return javaType;
    }
}
