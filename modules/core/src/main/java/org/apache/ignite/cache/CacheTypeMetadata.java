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

import org.apache.ignite.internal.util.tostring.*;

import java.util.*;

/**
 * Type metadata.
 */
public class CacheTypeMetadata {
    /** Schema name in database. */
    private String dbSchema;

    /** Table name in database. */
    private String dbTbl;

    /** Key class used to store key in cache. */
    private String keyType;

    /** Value class used to store value in cache. */
    private String valType;

    /** Key fields. */
    @GridToStringInclude
    private Collection<CacheTypeFieldMetadata> keyFields;

    /** Value fields . */
    @GridToStringInclude
    private Collection<CacheTypeFieldMetadata> valFields;

    /**
     * Gets database schema name.
     *
     * @return Schema name.
     */
    public String getDatabaseSchema() {
        return dbSchema;
    }

    /**
     * Sets database schema name.
     *
     * @param dbSchema Schema name.
     */
    public void setDatabaseSchema(String dbSchema) {
        this.dbSchema = dbSchema;
    }

    /**
     * Gets table name in database.
     *
     * @return Table name in database.
     */
    public String getDatabaseTable() {
        return dbTbl;
    }

    /**
     * Table name in database.
     *
     * @param dbTbl Table name in database.
     */
    public void setDatabaseTable(String dbTbl) {
        this.dbTbl = dbTbl;
    }

    /**
     * Gets key type.
     *
     * @return Key type.
     */
    public String getKeyType() {
        return keyType;
    }

    /**
     * Sets key type.
     *
     * @param keyType Key type.
     */
    public void setKeyType(String keyType) {
        this.keyType = keyType;
    }


    /**
     * Gets value type.
     *
     * @return Key type.
     */
    public String getValueType() {
        return valType;
    }

    /**
     * Sets value type.
     *
     * @param valType Value type.
     */
    public void setValueType(String valType) {
        this.valType = valType;
    }

    /**
     * Gets key fields.
     *
     * @return Key fields.
     */
    public Collection<CacheTypeFieldMetadata> getKeyFields() {
        return keyFields;
    }

    /**
     * Sets key fields.
     *
     * @param keyFields New key fields.
     */
    public void setKeyFields(Collection<CacheTypeFieldMetadata> keyFields) {
        this.keyFields = keyFields;
    }

    /**
     * Gets value fields.
     *
     * @return Value fields.
     */
    public Collection<CacheTypeFieldMetadata> getValueFields() {
        return valFields;
    }

    /**
     * Sets value fields.
     *
     * @param valFields New value fields.
     */
    public void setValueFields(Collection<CacheTypeFieldMetadata> valFields) {
        this.valFields = valFields;
    }
}
