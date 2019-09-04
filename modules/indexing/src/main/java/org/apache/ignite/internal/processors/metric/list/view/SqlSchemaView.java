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

package org.apache.ignite.internal.processors.metric.list.view;

import org.apache.ignite.spi.metric.list.MonitoringList;
import org.apache.ignite.spi.metric.list.MonitoringRow;

/**
 * Sql schema representation for a {@link MonitoringList}.
 */
public class SqlSchemaView implements MonitoringRow<String> {
    /** Schema name */
    private String name;

    /** {@code True} if schema is predefined, {@code false} otherwise. */
    private boolean predefined;

    /**
     * @param name Schema name.
     * @param predefined Predefined flag.
     */
    public SqlSchemaView(String name, boolean predefined) {
        this.name = name;
        this.predefined = predefined;
    }

    /** {@inheritDoc} */
    @Override public String monitoringRowId() {
        return name();
    }

    /** @return Schema name. */
    public String name() {
        return name;
    }

    /** @return {@code True} if schema is predefined, {@code false} otherwise. */
    public boolean predefined() {
        return predefined;
    }
}
