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
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Data transfer object for {@link org.apache.ignite.cache.QueryEntity}.
 */
public class QueryEntity extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Key class used to store key in cache. */
    private String keyType;

    /** Value class used to store value in cache. */
    private String valType;

    /** Fields to be queried, in addition to indexed fields. */
    private Map<String, String> qryFlds;

    /** Key fields. */
    private List<String> keyFields;

    /** Aliases. */
    private Map<String, String> aliases;

    /** Table name. */
    private String tblName;

    /** Key name. Can be used in field list to denote the key as a whole. */
    private String keyFieldName;

    /** Value name. Can be used in field list to denote the entire value. */
    private String valFieldName;

    /** Fields to create group indexes for. */
    private List<QueryIndex> grps;

    /**
     * @param qryEntities Collection of query entities.
     * @return Data transfer object for query entities.
     */
    public static List<QueryEntity> list(Collection<org.apache.ignite.cache.QueryEntity> qryEntities) {
        List<QueryEntity> entities = new ArrayList<>();

        // Add query entries.
        if (!F.isEmpty(qryEntities))
            for (org.apache.ignite.cache.QueryEntity qryEntity : qryEntities)
                entities.add(new QueryEntity(qryEntity));

        return entities;
    }

    /**
     * Create data transfer object for given cache type metadata.
     */
    public QueryEntity() {
        // No-op.
    }

    /**
     * Create data transfer object for given cache type metadata.
     *
     * @param q Actual cache query entities.
     */
    private QueryEntity(org.apache.ignite.cache.QueryEntity q) {
        assert q != null;

        keyType = q.getKeyType();
        valType = q.getValueType();

        keyFields = toList(q.getKeyFields());

        LinkedHashMap<String, String> qryFields = q.getFields();

        qryFlds = new LinkedHashMap<>(qryFields);

        aliases = new HashMap<>(q.getAliases());

        Collection<org.apache.ignite.cache.QueryIndex> qryIdxs = q.getIndexes();

        grps = new ArrayList<>(qryIdxs.size());

        for (org.apache.ignite.cache.QueryIndex qryIdx : qryIdxs)
            grps.add(new QueryIndex(qryIdx));

        tblName = q.getTableName();
        keyFieldName = q.getKeyFieldName();
        valFieldName = q.getValueFieldName();
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
    public List<String> getKeyFields() {
        return keyFields;
    }

    /**
     * @return Fields to be queried, in addition to indexed fields.
     */
    public Map<String, String> getQueryFields() {
        return qryFlds;
    }

    /**
     * @return Field aliases.
     */
    public Map<String, String> getAliases() {
        return aliases;
    }

    /**
     * @return Table name.
     */
    public String getTableName() {
        return tblName;
    }

    /**
     * @return Key name. Can be used in field list to denote the key as a whole.
     */
    public String getKeyFieldName() {
        return keyFieldName;
    }

    /**
     * @return Value name. Can be used in field list to denote the entire value.
     */
    public String getValueFieldName() {
        return valFieldName;
    }

    /**
     * @return Fields to create group indexes for.
     */
    public List<QueryIndex> getGroups() {
        return grps;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, keyType);
        U.writeString(out, valType);
        U.writeCollection(out, keyFields);
        IgniteUtils.writeStringMap(out, qryFlds);
        U.writeMap(out, aliases);
        U.writeCollection(out, grps);
        U.writeString(out, tblName);
        U.writeString(out, keyFieldName);
        U.writeString(out, valFieldName);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(ObjectInput in) throws IOException, ClassNotFoundException {
        keyType = U.readString(in);
        valType = U.readString(in);
        keyFields = U.readList(in);
        qryFlds = IgniteUtils.readStringMap(in);
        aliases = U.readMap(in);
        grps = U.readList(in);
        tblName = U.readString(in);
        keyFieldName = U.readString(in);
        valFieldName = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryEntity.class, this);
    }
}
