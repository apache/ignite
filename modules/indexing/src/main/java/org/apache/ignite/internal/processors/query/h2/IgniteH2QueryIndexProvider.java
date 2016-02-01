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
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.h2.index.Index;
import org.h2.table.IndexColumn;

/**
 *
 */
public interface IgniteH2QueryIndexProvider {
    /**
     * @param name Index name.
     * @param tbl Table to create index for.
     * @param pk Primary key index flag.
     * @param keyCol Key column index.
     * @param valCol Value column index.
     * @param cols Index columns.
     * @return Created index.
     */
    public Index createIndex(
        int cacheId,
        String name,
        GridH2Table tbl,
        boolean pk,
        int keyCol,
        int valCol,
        IndexColumn... cols) throws IgniteCheckedException;
}
