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
 *
 */

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Information about table.
 */
public class TableInformation {
    /** Cache group id. */
    private final int cacheGrpId;

    /** Cache group name. */
    private final String cacheGrpName;

    /** Cache id. */
    private final int cacheId;

    /** Cache name. */
    private final String cacheName;

    /** Affinity Key column. */
    private final String affinityKeyCol;

    /** Key alias. */
    private final String keyAlias;

    /** Value alias. */
    private final String valAlias;

    /** Key type name. */
    private final String keyTypeName;

    /** Value type name. */
    private final String valTypeName;

    /** Schema name. */
    private final String schemaName;

    /** Table name. */
    private final String tblName;

    /** Table type. */
    private final String tblType;

    /**
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @param tblType Table type.
     * @param cacheGrpId Cache group id.
     * @param cacheGrpName Cache group name.
     * @param cacheId Cache id.
     * @param cacheName Cache name.
     * @param affinityKeyCol Affinity key column name.
     * @param keyAlias Key alias.
     * @param valAlias Value alias.
     * @param keyTypeName Key type name.
     * @param valTypeName Value type name.
     */
    public TableInformation(String schemaName, String tblName,
        String tblType, int cacheGrpId, String cacheGrpName, int cacheId, String cacheName, String affinityKeyCol,
        String keyAlias, String valAlias, String keyTypeName, String valTypeName) {
        assert schemaName != null;
        assert tblName != null;
        assert tblType != null;

        this.schemaName = schemaName;
        this.tblName = tblName;
        this.tblType = tblType;

        this.cacheGrpId = cacheGrpId;
        this.cacheGrpName = cacheGrpName;
        this.cacheId = cacheId;
        this.cacheName = cacheName;
        this.affinityKeyCol = affinityKeyCol;
        this.keyAlias = keyAlias;
        this.valAlias = valAlias;
        this.keyTypeName = keyTypeName;
        this.valTypeName = valTypeName;
    }

    /**
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @param tblType Table type.
     */
    public TableInformation(String schemaName, String tblName, String tblType){
        assert schemaName != null;
        assert tblName != null;
        assert tblType != null;

        this.schemaName = schemaName;
        this.tblName = tblName;
        this.tblType = tblType;

        cacheGrpId = -1;
        cacheGrpName = null;
        cacheId = -1;
        cacheName = null;
        affinityKeyCol = null;
        keyAlias = null;
        valAlias = null;
        keyTypeName = null;
        valTypeName = null;
    }

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @return Table name.
     */
    public String tableName() {
        return tblName;
    }

    /**
     * @return Table type.
     */
    public String tableType() {
        return tblType;
    }

    /**
     * @return Cache Group id or {@code -1} if not applicable.
     */
    public int cacheGrpId() {
        return cacheGrpId;
    }

    /**
     * @return Cache group name or {@code null} if not applicable.
     */
    public String cacheGrpName() {
        return cacheGrpName;
    }

    /**
     * @return Cache id or {@code -1} if not applicable.
     */
    public int cacheId() {
        return cacheId;
    }

    /**
     * @return Cache name or {@code null} if not applicable.
     */
    public String cacheName() {
        return cacheName;
    }

    /**
     * @return Affinity key column name or {@code null} if not applicable.
     */
    @Nullable public String affinityKeyColumn() {
        return affinityKeyCol;
    }

    /**
     * @return Key alias or {@code null} if not applicable.
     */
    public String keyAlias() {
        return keyAlias;
    }

    /**
     * @return Value alias or {@code null} if not applicable.
     */
    public String valueAlias() {
        return valAlias;
    }

    /**
     * @return Key type name or {@code null} if not applicable.
     */
    public String keyTypeName() {
        return keyTypeName;
    }

    /**
     * @return Value type name or {@code null} if not applicable.
     */
    public String valueTypeName() {
        return valTypeName;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        TableInformation tblInfo = (TableInformation)o;

        return schemaName.equals(tblInfo.schemaName) && tblName.equals(tblInfo.tblName);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = schemaName.hashCode();

        res = 31 * res + tblName.hashCode();

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TableInformation.class, this);
    }
}
