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

package org.apache.ignite.internal.processors.query.h2.opt.join;

import java.util.List;
import java.util.concurrent.Future;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.h2.index.Cursor;
import org.h2.index.IndexLookupBatch;
import org.h2.result.SearchRow;

/**
 * Lookip batch for proxy indexes.
 */
public class ProxyDistributedLookupBatch implements IndexLookupBatch {
    /** Underlying normal lookup batch */
    private final IndexLookupBatch delegate;

    /** Row descriptor. */
    private final GridH2RowDescriptor rowDesc;

    /**
     * Creates proxy lookup batch.
     *
     * @param delegate Underlying index lookup batch.
     * @param rowDesc Row descriptor.
     */
    public ProxyDistributedLookupBatch(IndexLookupBatch delegate, GridH2RowDescriptor rowDesc) {
        this.delegate = delegate;
        this.rowDesc = rowDesc;
    }

    /** {@inheritDoc} */
    @Override public boolean addSearchRows(SearchRow first, SearchRow last) {
        SearchRow firstProxy = rowDesc.prepareProxyIndexRow(first);
        SearchRow lastProxy = rowDesc.prepareProxyIndexRow(last);

        return delegate.addSearchRows(firstProxy, lastProxy);
    }

    /** {@inheritDoc} */
    @Override public boolean isBatchFull() {
        return delegate.isBatchFull();
    }

    /** {@inheritDoc} */
    @Override public List<Future<Cursor>> find() {
        return delegate.find();
    }

    /** {@inheritDoc} */
    @Override public String getPlanSQL() {
        return delegate.getPlanSQL();
    }

    /** {@inheritDoc} */
    @Override public void reset(boolean beforeQuery) {
        delegate.reset(beforeQuery);
    }
}
