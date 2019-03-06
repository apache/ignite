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

package org.apache.ignite.cache.store.cassandra.session;

import org.apache.ignite.cache.store.cassandra.session.transaction.Mutation;

import java.io.Closeable;
import java.util.List;

/**
 * Wrapper around Cassandra driver session, to automatically handle:
 * <ul>
 *  <li>Keyspace and table absence exceptions</li>
 *  <li>Timeout exceptions</li>
 *  <li>Batch operations</li>
 * </ul>
 */
public interface CassandraSession extends Closeable {
    /**
     * Execute single synchronous operation against Cassandra  database.
     *
     * @param assistant execution assistance to perform the main operation logic.
     * @param <V> type of the result returned from operation.
     *
     * @return result of the operation.
     */
    public <V> V execute(ExecutionAssistant<V> assistant);

    /**
     * Executes batch asynchronous operation against Cassandra database.
     *
     * @param assistant execution assistance to perform the main operation logic.
     * @param data data which should be processed in batch operation.
     * @param <R> type of the result returned from batch operation.
     * @param <V> type of the value used in batch operation.
     *
     * @return result of the operation.
     */
    public <R, V> R execute(BatchExecutionAssistant<R, V> assistant, Iterable<? extends V> data);

    /**
     * Executes batch asynchronous operation to load bunch of records
     * specified by CQL statement from Cassandra database
     *
     * @param assistant execution assistance to perform the main operation logic.
     */
    public void execute(BatchLoaderAssistant assistant);

    /**
     * Executes all the mutations performed withing Ignite transaction against Cassandra database.
     *
     * @param mutations Mutations.
     */
    public void execute(List<Mutation> mutations);
}
