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

package org.apache.ignite.internal.processors.query.h2.sys.view;

import java.util.Iterator;
import org.apache.ignite.internal.GridKernalContext;
import org.h2.engine.Session;
import org.h2.result.Row;
import org.h2.result.SearchRow;

public class SqlSystemViewTables extends SqlAbstractLocalSystemView {
    @Override public Iterator<Row> getRows(Session ses, SearchRow first, SearchRow last) {
        return null;
    }

    public SqlSystemViewTables(GridKernalContext ctx) {
        super("TABLES", "Ignite tables", ctx, new String[] {"NAME", "OWNING_CACHE_ID"},
            newColumn("SQL_SCHEMA"),
            newColumn("TABLE_NAME"),
            newColumn("OWNING_CACHE_NAME"),
            newColumn("OWNING_CACHE_ID")
        );
    }
}
