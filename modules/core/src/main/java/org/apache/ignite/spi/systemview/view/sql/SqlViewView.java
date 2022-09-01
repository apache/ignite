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

package org.apache.ignite.spi.systemview.view.sql;

import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.spi.systemview.view.SystemView;

/**
 * Sql view representation for a {@link SystemView}.
 */
public class SqlViewView {
    /** System view. */
    private final SystemView<?> view;

    /** @param view System view. */
    public SqlViewView(SystemView<?> view) {
        this.view = view;
    }

    /** @return View name. */
    @Order
    public String name() {
        return MetricUtils.toSqlName(view.name());
    }

    /** @return View description. */
    @Order(2)
    public String description() {
        return view.description();
    }

    /** @return View schema. */
    @Order(1)
    public String schema() {
        return QueryUtils.SCHEMA_SYS;
    }
}
