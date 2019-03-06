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
