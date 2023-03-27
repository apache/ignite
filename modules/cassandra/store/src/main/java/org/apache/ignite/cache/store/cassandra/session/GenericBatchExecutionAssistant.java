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

package org.apache.ignite.cache.store.cassandra.session;

import java.util.HashSet;
import java.util.Set;
import com.datastax.driver.core.Row;

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
