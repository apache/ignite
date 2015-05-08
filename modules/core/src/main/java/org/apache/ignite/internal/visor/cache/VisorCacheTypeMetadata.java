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
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;

import java.io.*;
import java.util.*;

/**
 * Data transfer object for {@link CacheTypeMetadata}.
 */
public class VisorCacheTypeMetadata implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

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
    private Collection<VisorCacheTypeFieldMetadata> keyFields;

    /** Value fields. */
    @GridToStringInclude
    private Collection<VisorCacheTypeFieldMetadata> valFields;

    /** Fields to be queried, in addition to indexed fields. */
    @GridToStringInclude
    private Map<String, String> qryFlds;

    /** Fields to index in ascending order. */
    @GridToStringInclude
    private Map<String, String> ascFlds;

    /** Fields to index in descending order. */
    @GridToStringInclude
    private Map<String, String> descFlds;

    /** Fields to index as text. */
    @GridToStringInclude
    private Collection<String> txtFlds;

    /** Fields to create group indexes for. */
    @GridToStringInclude
    private Map<String, LinkedHashMap<String, IgniteBiTuple<String, Boolean>>> grps;

    /**
     * @param types Cache types metadata configurations.
     * @return Data transfer object for cache type metadata configurations.
     */
    public static Collection<VisorCacheTypeMetadata> list(Collection<CacheTypeMetadata> types) {
        if (types == null)
            return Collections.emptyList();

        final Collection<VisorCacheTypeMetadata> cfgs = new ArrayList<>(types.size());

        for (CacheTypeMetadata type : types)
            cfgs.add(from(type));

        return cfgs;
    }

    /**
     * @param m Actual cache type metadata.
     * @return Data transfer object for given cache type metadata.
     */
    public static VisorCacheTypeMetadata from(CacheTypeMetadata m) {
        assert m != null;

        VisorCacheTypeMetadata metadata = new VisorCacheTypeMetadata();

        metadata.dbSchema(m.getDatabaseSchema());
        metadata.dbTbl(m.getDatabaseTable());
        metadata.keyType(m.getKeyType());
        metadata.valType(m.getValueType());

        ArrayList<VisorCacheTypeFieldMetadata> fields = new ArrayList<>(m.getKeyFields().size());

        for (CacheTypeFieldMetadata field : m.getKeyFields())
            fields.add(VisorCacheTypeFieldMetadata.from(field));

        metadata.keyFields(fields);

        fields = new ArrayList<>(m.getValueFields().size());

        for (CacheTypeFieldMetadata field : m.getValueFields())
            fields.add(VisorCacheTypeFieldMetadata.from(field));

        metadata.valFields(fields);

        metadata.qryFlds(convertFieldsMap(m.getQueryFields()));
        metadata.ascFlds(convertFieldsMap(m.getAscendingFields()));
        metadata.descFlds(convertFieldsMap(m.getDescendingFields()));
        metadata.txtFlds(m.getTextFields());
        metadata.grps(convertGrpsMap(m.getGroups()));

        return metadata;
    }

    /**
     * Convert class object to string class name in the fields map.
     *
     * @param base Map with class object.
     * @return Map with string class name.
     */
    private static Map<String, String> convertFieldsMap(Map<String, Class<?>> base) {
        Map<String, String> res = new LinkedHashMap<>(base.size());

        for (Map.Entry<String, Class<?>> e : base.entrySet())
            res.put(e.getKey(), U.compact(e.getValue().getName()));

        return res;
    }

    /**
     * Convert class object to string class name in the  groups map.
     *
     * @param base Map with class object.
     * @return Map with string class name.
     */
    private static Map<String, LinkedHashMap<String, IgniteBiTuple<String, Boolean>>> convertGrpsMap(
        Map<String, LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>>> base) {
        Map<String, LinkedHashMap<String, IgniteBiTuple<String, Boolean>>> res = new LinkedHashMap<>(base.size());

        for (Map.Entry<String, LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>>> e : base.entrySet()) {
            LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>> intBase = e.getValue();
            LinkedHashMap<String, IgniteBiTuple<String, Boolean>> intRes = new LinkedHashMap<>(intBase.size());

            for (Map.Entry<String, IgniteBiTuple<Class<?>, Boolean>> intE : intBase.entrySet()) {
                IgniteBiTuple<Class<?>, Boolean> val = intE.getValue();

                intRes.put(intE.getKey(), new IgniteBiTuple<>(U.compact(val.get1().getName()), val.get2()));
            }

            res.put(e.getKey(), intRes);
        }

        return res;
    }

    /**
     * @param dbSchema New schema name in database.
     */
    public void dbSchema(String dbSchema) {
        this.dbSchema = dbSchema;
    }

    /**
     * @return Schema name in database.
     */
    public String dbSchema() {
        return dbSchema;
    }

    /**
     * @param dbTbl New table name in database.
     */
    public void dbTbl(String dbTbl) {
        this.dbTbl = dbTbl;
    }

    /**
     * @return Table name in database.
     */
    public String dbTbl() {
        return dbTbl;
    }

    /**
     * @param keyType New key class used to store key in cache.
     */
    public void keyType(String keyType) {
        this.keyType = keyType;
    }

    /**
     * @return Key class used to store key in cache.
     */
    public String keyType() {
        return keyType;
    }

    /**
     * @param valType New value class used to store value in cache.
     */
    public void valType(String valType) {
        this.valType = valType;
    }

    /**
     * @return Value class used to store value in cache.
     */
    public String valType() {
        return valType;
    }

    /**
     * @param keyFields New key fields.
     */
    public void keyFields(Collection<VisorCacheTypeFieldMetadata> keyFields) {
        this.keyFields = keyFields;
    }

    /**
     * @return Key fields.
     */
    public Collection<VisorCacheTypeFieldMetadata> keyFields() {
        return keyFields;
    }

    /**
     * @param valFields New value fields.
     */
    public void valFields(Collection<VisorCacheTypeFieldMetadata> valFields) {
        this.valFields = valFields;
    }

    /**
     * @return Value fields.
     */
    public Collection<VisorCacheTypeFieldMetadata> valFields() {
        return valFields;
    }

    /**
     * @param qryFlds New fields to be queried, in addition to indexed fields.
     */
    public void qryFlds(Map<String, String> qryFlds) {
        this.qryFlds = qryFlds;
    }

    /**
     * @return Fields to be queried, in addition to indexed fields.
     */
    public Map<String, String> qryFlds() {
        return qryFlds;
    }

    /**
     * @param ascFlds New fields to index in ascending order.
     */
    public void ascFlds(Map<String, String> ascFlds) {
        this.ascFlds = ascFlds;
    }

    /**
     * @return Fields to index in ascending order.
     */
    public Map<String, String> ascFlds() {
        return ascFlds;
    }

    /**
     * @param descFlds New fields to index in descending order.
     */
    public void descFlds(Map<String, String> descFlds) {
        this.descFlds = descFlds;
    }

    /**
     * @return Fields to index in descending order.
     */
    public Map<String, String> descFlds() {
        return descFlds;
    }

    /**
     * @param txtFlds New fields to index as text.
     */
    public void txtFlds(Collection<String> txtFlds) {
        this.txtFlds = txtFlds;
    }

    /**
     * @return Fields to index as text.
     */
    public Collection<String> txtFlds() {
        return txtFlds;
    }

    /**
     * @param grps New fields to create group indexes for.
     */
    public void grps(Map<String, LinkedHashMap<String, IgniteBiTuple<String, Boolean>>> grps) {
        this.grps = grps;
    }

    /**
     * @return Fields to create group indexes for.
     */
    public Map<String, LinkedHashMap<String, IgniteBiTuple<String, Boolean>>> grps() {
        return grps;
    }
}
