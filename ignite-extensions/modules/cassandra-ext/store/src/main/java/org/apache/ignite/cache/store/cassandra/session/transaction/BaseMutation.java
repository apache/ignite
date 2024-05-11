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

import org.apache.ignite.cache.store.cassandra.persistence.KeyValuePersistenceSettings;
import org.apache.ignite.cache.store.cassandra.persistence.PersistenceController;

/**
 * Base class to inherit from to implement specific mutations operation.
 */
public abstract class BaseMutation implements Mutation {
    /** Cassandra table to use. */
    private final String table;

    /** Persistence controller to be utilized for mutation. */
    private final PersistenceController ctrl;

    /**
     * Creates instance of mutation operation.
     *
     * @param table Cassandra table which should be used for the mutation.
     * @param ctrl Persistence controller to use.
     */
    public BaseMutation(String table, PersistenceController ctrl) {
        if (table == null || table.trim().isEmpty())
            throw new IllegalArgumentException("Table name should be specified");

        if (ctrl == null)
            throw new IllegalArgumentException("Persistence controller should be specified");

        this.table = table;
        this.ctrl = ctrl;
    }

    /** {@inheritDoc} */
    @Override public String getTable() {
        return table;
    }

    /** {@inheritDoc} */
    @Override public KeyValuePersistenceSettings getPersistenceSettings() {
        return ctrl.getPersistenceSettings();
    }

    /**
     * Service method to get persistence controller instance
     *
     * @return Persistence controller to use for the mutation
     */
    protected PersistenceController controller() {
        return ctrl;
    }
}
