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

package org.apache.ignite.cache.query;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.internal.processors.bulkload.BulkLoadAckClientParameters;
import org.apache.ignite.internal.processors.bulkload.BulkLoadProcessor;
import org.jetbrains.annotations.NotNull;

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
