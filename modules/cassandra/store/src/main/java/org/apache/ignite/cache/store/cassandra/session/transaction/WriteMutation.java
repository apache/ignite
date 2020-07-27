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

package org.apache.ignite.cache.store.cassandra.session.transaction;

import javax.cache.Cache;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import org.apache.ignite.cache.store.cassandra.persistence.PersistenceController;

/**
 * Mutation which writes(inserts) object into Cassandra.
 */
public class WriteMutation extends BaseMutation {
    /** Ignite cache entry to be inserted into Cassandra. */
    private final Cache.Entry entry;

    /**
     * Creates instance of delete mutation operation.
     *
     * @param entry Ignite cache entry to be inserted into Cassandra.
     * @param table Cassandra table which should be used for the mutation.
     * @param ctrl Persistence controller to use.
     */
    public WriteMutation(Cache.Entry entry, String table, PersistenceController ctrl) {
        super(table, ctrl);
        this.entry = entry;
    }

    /** {@inheritDoc} */
    @Override public boolean tableExistenceRequired() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public String getStatement() {
        return controller().getWriteStatement(getTable());
    }

    /** {@inheritDoc} */
    @Override public BoundStatement bindStatement(PreparedStatement statement) {
        return controller().bindKeyValue(statement, entry.getKey(), entry.getValue());
    }
}
