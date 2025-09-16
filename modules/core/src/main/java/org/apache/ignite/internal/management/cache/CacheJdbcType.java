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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import javax.cache.configuration.Factory;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory;
import org.apache.ignite.cache.store.jdbc.JdbcType;
import org.apache.ignite.cache.store.jdbc.JdbcTypeField;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Data transfer object for {@link JdbcType}.
 */
public class CacheJdbcType extends IgniteDataTransferObject {
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
    private List<CacheJdbcTypeField> keyFields;

    /** Value fields. */
    private List<CacheJdbcTypeField> valFields;

    /**
     * @param factory Store factory to extract JDBC types info.
     * @return Data transfer object for cache type metadata configurations.
     */
    public static List<CacheJdbcType> list(Factory factory) {
        List<CacheJdbcType> res = new ArrayList<>();

        if (factory instanceof CacheJdbcPojoStoreFactory) {
            CacheJdbcPojoStoreFactory jdbcFactory = (CacheJdbcPojoStoreFactory)factory;

            JdbcType[] jdbcTypes = jdbcFactory.getTypes();

            if (!F.isEmpty(jdbcTypes)) {
                for (JdbcType jdbcType : jdbcTypes)
                    res.add(new CacheJdbcType(jdbcType));
            }
        }

        return res;
    }

    /**
     * Create data transfer object for given cache type metadata.
     */
    public CacheJdbcType() {
        // No-op.
    }

    /**
     * Create data transfer object for given cache type metadata.
     *
     * @param jdbcType JDBC type.
     */
    public CacheJdbcType(JdbcType jdbcType) {
        keyType = jdbcType.getKeyType();
        valType = jdbcType.getValueType();

        dbSchema = jdbcType.getDatabaseSchema();
        dbTbl = jdbcType.getDatabaseTable();

        JdbcTypeField[] kFields = jdbcType.getKeyFields();

        if (kFields != null) {
            keyFields = new ArrayList<>(kFields.length);

            for (JdbcTypeField fld : kFields)
                keyFields.add(new CacheJdbcTypeField(
                    fld.getDatabaseFieldName(), fld.getDatabaseFieldType(),
                    fld.getDatabaseFieldName(), U.compact(fld.getJavaFieldType().getName())));
        }

        JdbcTypeField[] vFields = jdbcType.getValueFields();

        if (vFields != null) {
            valFields = new ArrayList<>(vFields.length);

            for (JdbcTypeField fld : vFields)
                valFields.add(new CacheJdbcTypeField(
                    fld.getDatabaseFieldName(), fld.getDatabaseFieldType(),
                    fld.getDatabaseFieldName(), U.compact(fld.getJavaFieldType().getName())));
        }
    }

    /**
     * @return Schema name in database.
     */
    public String getDatabaseSchema() {
        return dbSchema;
    }

    /**
     * @return Table name in database.
     */
    public String getDatabaseTable() {
        return dbTbl;
    }

    /**
     * @return Key class used to store key in cache.
     */
    public String getKeyType() {
        return keyType;
    }

    /**
     * @return Value class used to store value in cache.
     */
    public String getValueType() {
        return valType;
    }

    /**
     * @return Key fields.
     */
    public List<CacheJdbcTypeField> getKeyFields() {
        return keyFields;
    }

    /**
     * @return Value fields.
     */
    public List<CacheJdbcTypeField> getValueFields() {
        return valFields;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, dbSchema);
        U.writeString(out, dbTbl);
        U.writeString(out, keyType);
        U.writeString(out, valType);
        U.writeCollection(out, keyFields);
        U.writeCollection(out, valFields);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(ObjectInput in) throws IOException, ClassNotFoundException {
        dbSchema = U.readString(in);
        dbTbl = U.readString(in);
        keyType = U.readString(in);
        valType = U.readString(in);
        keyFields = U.readList(in);
        valFields = U.readList(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheJdbcType.class, this);
    }
}
