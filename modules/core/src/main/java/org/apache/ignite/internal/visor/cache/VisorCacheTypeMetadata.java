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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.ignite.cache.CacheTypeFieldMetadata;
import org.apache.ignite.cache.CacheTypeMetadata;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory;
import org.apache.ignite.cache.store.jdbc.JdbcType;
import org.apache.ignite.cache.store.jdbc.JdbcTypeField;
import org.apache.ignite.internal.LessNamingBean;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;

import javax.cache.configuration.Factory;

/**
 * Data transfer object for {@link CacheTypeMetadata}.
 */
public class VisorCacheTypeMetadata implements Serializable, LessNamingBean {
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
     * @param qryEntities Collection of query entities.
     * @param factory Store factory to extract JDBC types info.
     * @param types Cache types metadata configurations.
     * @return Data transfer object for cache type metadata configurations.
     */
    public static Collection<VisorCacheTypeMetadata> list(Collection<QueryEntity> qryEntities, Factory factory,
        Collection<CacheTypeMetadata> types) {
        final Collection<VisorCacheTypeMetadata> metas = new ArrayList<>();

        Map<String, VisorCacheTypeMetadata> metaMap =
                U.newHashMap(qryEntities != null ? qryEntities.size() : 0);

        // Add query entries.
        if (qryEntities != null)
            for (QueryEntity qryEntity : qryEntities) {
                VisorCacheTypeMetadata meta = from(qryEntity);

                metas.add(meta);

                metaMap.put(meta.keyType, meta);
            }

        // Add JDBC types.
        if (factory != null && factory instanceof CacheJdbcPojoStoreFactory) {
             CacheJdbcPojoStoreFactory jdbcFactory = (CacheJdbcPojoStoreFactory) factory;

            JdbcType[] jdbcTypes = jdbcFactory.getTypes();

            if (jdbcTypes != null && jdbcTypes.length > 0) {
                for (JdbcType jdbcType : jdbcTypes) {
                    VisorCacheTypeMetadata meta = metaMap.get(jdbcType.getKeyType());

                    boolean notFound = meta == null;

                    if (notFound) {
                        meta = new VisorCacheTypeMetadata();

                        meta.keyType = jdbcType.getKeyType();
                        meta.valType = jdbcType.getValueType();

                        meta.qryFlds = Collections.emptyMap();
                        meta.ascFlds = Collections.emptyMap();
                        meta.descFlds = Collections.emptyMap();
                        meta.txtFlds = Collections.emptyList();
                        meta.grps = Collections.emptyMap();
                    }

                    meta.dbSchema = jdbcType.getDatabaseSchema();
                    meta.dbTbl = jdbcType.getDatabaseTable();

                    JdbcTypeField[] keyFields = jdbcType.getKeyFields();

                    if (keyFields != null) {
                        meta.keyFields = new ArrayList<>(keyFields.length);

                        for (JdbcTypeField fld : keyFields)
                            meta.keyFields.add(new VisorCacheTypeFieldMetadata(
                                fld.getDatabaseFieldName(), fld.getDatabaseFieldType(),
                                fld.getDatabaseFieldName(), U.compact(fld.getJavaFieldType().getName())));
                    }

                    JdbcTypeField[] valFields = jdbcType.getValueFields();

                    if (valFields != null) {
                        meta.valFields = new ArrayList<>(valFields.length);

                        for (JdbcTypeField fld : valFields)
                            meta.valFields.add(new VisorCacheTypeFieldMetadata(
                                fld.getDatabaseFieldName(), fld.getDatabaseFieldType(),
                                fld.getDatabaseFieldName(), U.compact(fld.getJavaFieldType().getName())));
                    }

                    if (notFound)
                        metas.add(meta);
                }
            }
        }

        // Add old deprecated CacheTypeMetadata for compatibility.
        if (types != null)
            for (CacheTypeMetadata type : types)
                metas.add(from(type));

        return metas;
    }

    /**
     * @param q Actual cache query entities.
     * @return Data transfer object for given cache type metadata.
     */
    public static VisorCacheTypeMetadata from(QueryEntity q) {
        assert q != null;

        VisorCacheTypeMetadata metadata = new VisorCacheTypeMetadata();

        metadata.keyType = q.getKeyType();
        metadata.valType = q.getValueType();

        metadata.dbSchema = "";
        metadata.dbTbl = "";

        metadata.keyFields = Collections.emptyList();
        metadata.valFields = Collections.emptyList();

        LinkedHashMap<String, String> qryFields = q.getFields();

        metadata.qryFlds = new LinkedHashMap<>(qryFields);

        metadata.ascFlds = Collections.emptyMap();
        metadata.descFlds = Collections.emptyMap();
        metadata.txtFlds = Collections.emptyList();

        Collection<QueryIndex> qryIdxs = q.getIndexes();

        metadata.grps = new LinkedHashMap<>(qryIdxs.size());

        for (QueryIndex qryIdx : qryIdxs) {
            LinkedHashMap<String, Boolean> qryIdxFlds = qryIdx.getFields();

            LinkedHashMap<String, IgniteBiTuple<String, Boolean>> grpFlds = new LinkedHashMap<>();

            for (Map.Entry<String, Boolean> qryIdxFld : qryIdxFlds.entrySet()) {
                String fldName = qryIdxFld.getKey();

                grpFlds.put(fldName, new IgniteBiTuple<>(qryFields.get(fldName), !qryIdxFld.getValue()));
            }

            metadata.grps.put(qryIdx.getName(), grpFlds);
        }

        return metadata;
    }

    /**
     * @param m Actual cache type metadata.
     * @return Data transfer object for given cache type metadata.
     */
    public static VisorCacheTypeMetadata from(CacheTypeMetadata m) {
        assert m != null;

        VisorCacheTypeMetadata metadata = new VisorCacheTypeMetadata();

        metadata.dbSchema = m.getDatabaseSchema();
        metadata.dbTbl = m.getDatabaseTable();
        metadata.keyType = m.getKeyType();
        metadata.valType = m.getValueType();

        ArrayList<VisorCacheTypeFieldMetadata> fields = new ArrayList<>(m.getKeyFields().size());

        for (CacheTypeFieldMetadata field : m.getKeyFields())
            fields.add(VisorCacheTypeFieldMetadata.from(field));

        metadata.keyFields = fields;

        fields = new ArrayList<>(m.getValueFields().size());

        for (CacheTypeFieldMetadata field : m.getValueFields())
            fields.add(VisorCacheTypeFieldMetadata.from(field));

        metadata.valFields = fields;

        metadata.qryFlds = convertFieldsMap(m.getQueryFields());
        metadata.ascFlds = convertFieldsMap(m.getAscendingFields());
        metadata.descFlds = convertFieldsMap(m.getDescendingFields());
        metadata.txtFlds = m.getTextFields();
        metadata.grps = convertGrpsMap(m.getGroups());

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
     * @return Schema name in database.
     */
    public String dbSchema() {
        return dbSchema;
    }

    /**
     * @return Table name in database.
     */
    public String dbTbl() {
        return dbTbl;
    }

    /**
     * @return Key class used to store key in cache.
     */
    public String keyType() {
        return keyType;
    }

    /**
     * @return Value class used to store value in cache.
     */
    public String valType() {
        return valType;
    }

    /**
     * @return Key fields.
     */
    public Collection<VisorCacheTypeFieldMetadata> keyFields() {
        return keyFields;
    }

    /**
     * @return Value fields.
     */
    public Collection<VisorCacheTypeFieldMetadata> valFields() {
        return valFields;
    }

    /**
     * @return Fields to be queried, in addition to indexed fields.
     */
    public Map<String, String> qryFlds() {
        return qryFlds;
    }

    /**
     * @return Fields to index in ascending order.
     */
    public Map<String, String> ascFlds() {
        return ascFlds;
    }

    /**
     * @return Fields to index in descending order.
     */
    public Map<String, String> descFlds() {
        return descFlds;
    }

    /**
     * @return Fields to index as text.
     */
    public Collection<String> txtFlds() {
        return txtFlds;
    }

    /**
     * @return Fields to create group indexes for.
     */
    public Map<String, LinkedHashMap<String, IgniteBiTuple<String, Boolean>>> grps() {
        return grps;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheTypeMetadata.class, this);
    }
}
