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
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryManager;
import org.apache.ignite.internal.processors.query.schema.SchemaIndexCacheVisitorClosure;

/** */
class RebuildIndexFromHashClosure implements SchemaIndexCacheVisitorClosure {
    /** */
    private final GridCacheQueryManager qryMgr;

    /** MVCC status flag. */
    private final boolean mvccEnabled;

    /**
     * @param qryMgr Query manager.
     * @param mvccEnabled MVCC status flag.
     */
    RebuildIndexFromHashClosure(GridCacheQueryManager qryMgr, boolean mvccEnabled) {
        this.qryMgr = qryMgr;
        this.mvccEnabled = mvccEnabled;
    }

    /** {@inheritDoc} */
    @Override public void apply(CacheDataRow row) throws IgniteCheckedException {
        // prevRowAvailable is always true with MVCC on, and always false *on index rebuild* with MVCC off.
        qryMgr.store(row, null, mvccEnabled);
    }
}
