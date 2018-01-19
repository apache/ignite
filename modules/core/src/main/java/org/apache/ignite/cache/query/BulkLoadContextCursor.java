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

import org.apache.ignite.internal.processors.odbc.jdbc.JdbcBulkLoadContext;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/** FIXME SHQ */
public class BulkLoadContextCursor implements FieldsQueryCursor<List<?>> {

    private JdbcBulkLoadContext bulkLoadContext;

    public BulkLoadContextCursor(JdbcBulkLoadContext bulkLoadContext) {
        this.bulkLoadContext = bulkLoadContext;
    }

    public JdbcBulkLoadContext bulkLoadContext() {
        return bulkLoadContext;
    }

    @Override public List<List<?>> getAll() {
        return Collections.singletonList(Collections.singletonList(bulkLoadContext()));
    }

    @NotNull @Override public Iterator<List<?>> iterator() {
        return getAll().iterator();
    }

    @Override public void close() {
        // no-op
    }

    @Override public String getFieldName(int idx) {
        return "bulkLoadCursor"; // dummy stub
    }

    @Override public int getColumnsCount() {
        return 1; // dummy stub
    }
}
