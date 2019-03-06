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

import com.datastax.driver.core.Row;
import java.util.HashSet;
import java.util.Set;

/**
 * Implementation of the {@link org.apache.ignite.cache.store.cassandra.session.BatchExecutionAssistant}.
 *
 * @param <R> Type of the result returned from batch operation
 * @param <V> Type of the value used in batch operation
 */
public abstract class GenericBatchExecutionAssistant<R, V> implements BatchExecutionAssistant<R, V> {
    /** Identifiers of already processed objects. */
    private Set<Integer> processed = new HashSet<>();

    /** {@inheritDoc} */
    @Override public void process(Row row, int seqNum) {
        if (processed.contains(seqNum))
            return;

        process(row);

        processed.add(seqNum);
    }

    /** {@inheritDoc} */
    @Override public boolean alreadyProcessed(int seqNum) {
        return processed.contains(seqNum);
    }

    /** {@inheritDoc} */
    @Override public int processedCount() {
        return processed.size();
    }

    /** {@inheritDoc} */
    @Override public R processedData() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean tableExistenceRequired() {
        return false;
    }

    /**
     * Processes particular row inside batch operation.
     *
     * @param row Row to process.
     */
    protected void process(Row row) {
    }
}
