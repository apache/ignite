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

package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2IndexBase;
import org.apache.ignite.internal.processors.query.h2.opt.H2CacheRow;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2RowDescriptor;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorClosure;

/**
 * Index build closure.
 */
public class IndexBuildClosure implements SchemaIndexCacheVisitorClosure {
    /** Row descriptor. */
    private final GridH2RowDescriptor rowDesc;

    /** Index. */
    private final GridH2IndexBase idx;

    /**
     * Constructor.
     *
     * @param rowDesc Row descriptor.
     * @param idx Target index.
     */
    public IndexBuildClosure(GridH2RowDescriptor rowDesc, GridH2IndexBase idx) {
        this.rowDesc = rowDesc;
        this.idx = idx;
    }

    /** {@inheritDoc} */
    @Override public void apply(CacheDataRow row) throws IgniteCheckedException {
        H2CacheRow row0 = rowDesc.createRow(row);

        idx.putx(row0);
    }
}
