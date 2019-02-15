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

package org.apache.ignite.cache.query;

import org.apache.ignite.internal.processors.bulkload.BulkLoadAckClientParameters;
import org.apache.ignite.internal.processors.bulkload.BulkLoadProcessor;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * A special FieldsQueryCursor subclass that is used as a sentinel to transfer data from bulk load
 * (COPY) command to the JDBC or other client-facing driver: the bulk load batch processor
 * and parameters to send to the client.
 * */
public class BulkLoadContextCursor implements FieldsQueryCursor<List<?>> {
    /** Bulk load context from SQL command. */
    private final BulkLoadProcessor processor;

    /** Bulk load parameters to send to the client. */
    private final BulkLoadAckClientParameters clientParams;

    /**
     * Creates a cursor.
     *
     * @param processor Bulk load context object to store.
     * @param clientParams Parameters to send to client.
     */
    public BulkLoadContextCursor(BulkLoadProcessor processor, BulkLoadAckClientParameters clientParams) {
        this.processor = processor;
        this.clientParams = clientParams;
    }

    /**
     * Returns a bulk load context.
     *
     * @return a bulk load context.
     */
    public BulkLoadProcessor bulkLoadProcessor() {
        return processor;
    }

    /**
     * Returns the bulk load parameters to send to the client.
     *
     * @return The bulk load parameters to send to the client.
     */
    public BulkLoadAckClientParameters clientParams() {
        return clientParams;
    }

    /** {@inheritDoc} */
    @Override public List<List<?>> getAll() {
        return Collections.singletonList(Arrays.asList(processor, clientParams));
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<List<?>> iterator() {
        return getAll().iterator();
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // no-op
    }

    /** {@inheritDoc} */
    @Override public String getFieldName(int idx) {
        if (idx < 0 || idx > 1)
            throw new IndexOutOfBoundsException();

        return idx == 0 ? "processor" : "clientParams";
    }

    /** {@inheritDoc} */
    @Override public int getColumnsCount() {
        return 2;
    }
}
