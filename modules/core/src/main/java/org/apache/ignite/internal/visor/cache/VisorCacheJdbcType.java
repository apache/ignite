/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.visor.cache;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.cache.store.jdbc.CacheJdbcPojoStoreFactory;
import org.apache.ignite.cache.store.jdbc.JdbcType;
import org.apache.ignite.cache.store.jdbc.JdbcTypeField;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

import javax.cache.configuration.Factory;

/**
 * Data transfer object for {@link JdbcType}.
 */
public class VisorCacheJdbcType extends VisorDataTransferObject {
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
    private List<VisorCacheJdbcTypeField> keyFields;

    /** Value fields. */
    private List<VisorCacheJdbcTypeField> valFields;

    /**
     * @param factory Store factory to extract JDBC types info.
     * @return Data transfer object for cache type metadata configurations.
     */
    public static List<VisorCacheJdbcType> list(Factory factory) {
        List<VisorCacheJdbcType> res = new ArrayList<>();

        if (factory instanceof CacheJdbcPojoStoreFactory) {
            CacheJdbcPojoStoreFactory jdbcFactory = (CacheJdbcPojoStoreFactory) factory;

            JdbcType[] jdbcTypes = jdbcFactory.getTypes();

            if (!F.isEmpty(jdbcTypes)) {
                for (JdbcType jdbcType : jdbcTypes)
                    res.add(new VisorCacheJdbcType(jdbcType));
            }
        }

        return res;
    }

    /**
     * Create data transfer object for given cache type metadata.
     */
    public VisorCacheJdbcType() {
        // No-op.
    }

    /**
     * Create data transfer object for given cache type metadata.
     *
     * @param jdbcType JDBC type.
     */
    public VisorCacheJdbcType(JdbcType jdbcType) {
        keyType = jdbcType.getKeyType();
        valType = jdbcType.getValueType();

        dbSchema = jdbcType.getDatabaseSchema();
        dbTbl = jdbcType.getDatabaseTable();

        JdbcTypeField[] kFields = jdbcType.getKeyFields();

        if (kFields != null) {
            keyFields = new ArrayList<>(kFields.length);

            for (JdbcTypeField fld : kFields)
                keyFields.add(new VisorCacheJdbcTypeField(
                    fld.getDatabaseFieldName(), fld.getDatabaseFieldType(),
                    fld.getDatabaseFieldName(), U.compact(fld.getJavaFieldType().getName())));
        }

        JdbcTypeField[] vFields = jdbcType.getValueFields();

        if (vFields != null) {
            valFields = new ArrayList<>(vFields.length);

            for (JdbcTypeField fld : vFields)
                valFields.add(new VisorCacheJdbcTypeField(
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
    public List<VisorCacheJdbcTypeField> getKeyFields() {
        return keyFields;
    }

    /**
     * @return Value fields.
     */
    public List<VisorCacheJdbcTypeField> getValueFields() {
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
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        dbSchema = U.readString(in);
        dbTbl = U.readString(in);
        keyType = U.readString(in);
        valType = U.readString(in);
        keyFields = U.readList(in);
        valFields = U.readList(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorCacheJdbcType.class, this);
    }
}
