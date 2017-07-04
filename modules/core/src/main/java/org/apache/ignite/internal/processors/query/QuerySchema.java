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

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.internal.processors.query.schema.message.SchemaFinishDiscoveryMessage;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaAbstractOperation;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaIndexCreateOperation;
import org.apache.ignite.internal.processors.query.schema.operation.SchemaIndexDropOperation;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Dynamic cache schema.
 */
public class QuerySchema implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Query entities. */
    private final Collection<QueryEntity> entities = new LinkedList<>();

    /** Mutex for state synchronization. */
    private final Object mux = new Object();

    /**
     * Default constructor.
     */
    public QuerySchema() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param entities Query entities.
     */
    public QuerySchema(Collection<QueryEntity> entities) {
        assert entities != null;

        for (QueryEntity qryEntity : entities)
            this.entities.add(new QueryEntity(qryEntity));
    }

    /**
     * Copy object.
     *
     * @return Copy.
     */
    public QuerySchema copy() {
        synchronized (mux) {
            QuerySchema res = new QuerySchema();

            for (QueryEntity qryEntity : entities)
                res.entities.add(new QueryEntity(qryEntity));

            return res;
        }
    }

    /**
     * Process finish message.
     *
     * @param msg Message.
     */
    public void finish(SchemaFinishDiscoveryMessage msg) {
        synchronized (mux) {
            SchemaAbstractOperation op = msg.operation();

            if (op instanceof SchemaIndexCreateOperation) {
                SchemaIndexCreateOperation op0 = (SchemaIndexCreateOperation)op;

                for (QueryEntity entity : entities) {
                    String tblName = entity.getTableName();

                    if (F.eq(tblName, op0.tableName())) {
                        boolean exists = false;

                        for (QueryIndex idx : entity.getIndexes()) {
                            if (F.eq(idx.getName(), op0.indexName())) {
                                exists = true;

                                break;
                            }
                        }

                        if (!exists) {
                            List<QueryIndex> idxs = new ArrayList<>(entity.getIndexes());

                            idxs.add(op0.index());

                            entity.setIndexes(idxs);
                        }

                        break;
                    }
                }
            }
            else {
                assert op instanceof SchemaIndexDropOperation;

                SchemaIndexDropOperation op0 = (SchemaIndexDropOperation)op;

                for (QueryEntity entity : entities) {
                    Collection<QueryIndex> idxs = entity.getIndexes();

                    QueryIndex victim = null;

                    for (QueryIndex idx : idxs) {
                        if (F.eq(idx.getName(), op0.indexName())) {
                            victim = idx;

                            break;
                        }
                    }

                    if (victim != null) {
                        List<QueryIndex> newIdxs = new ArrayList<>(entity.getIndexes());

                        newIdxs.remove(victim);

                        entity.setIndexes(newIdxs);

                        break;
                    }
                }
            }
        }
    }

    /**
     * @return Query entities.
     */
    public Collection<QueryEntity> entities() {
        synchronized (mux) {
            return new ArrayList<>(entities);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QuerySchema.class, this);
    }
}
