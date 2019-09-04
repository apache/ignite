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

import org.apache.ignite.internal.processors.metric.list.walker.Order;
import org.apache.ignite.spi.metric.list.MonitoringList;
import org.apache.ignite.spi.metric.list.MonitoringRow;

/**
 * Sql table representation for a {@link MonitoringList}.
 */
public interface SqlTableView extends MonitoringRow<String> {
    /** {@inheritDoc} */
    @Override default String monitoringRowId() {
        return identifierString();
    }

    /** @return Cache group id. */
    public int cacheGroupId();

    /** @return Cache group name. */
    public String cacheGroupName();

    /** @return Cache id. */
    public int cacheId();

    /** @return Cache name. */
    @Order(2)
    public String cacheName();

    /** @return Schema name. */
    @Order(1)
    public String schemaName();

    /** @return Table name. */
    @Order
    public String tableName();

    /** @return Table identifier as string. */
    public String identifierString();

    /** @return Affinity key column. */
    public String affinityKeyColumn();

    /** @return Key alias. */
    public String keyAlias();

    /** @return Value alias. */
    public String valueAlias();

    /** @return Key type name. */
    public String keyTypeName();

    /** @return Value type name. */
    public String valueTypeName();
}
