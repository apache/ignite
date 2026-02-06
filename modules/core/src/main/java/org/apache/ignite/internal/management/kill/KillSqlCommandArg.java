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

package org.apache.ignite.internal.management.kill;

import java.util.UUID;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.Positional;
import org.apache.ignite.internal.util.typedef.T2;

import static org.apache.ignite.internal.QueryMXBeanImpl.EXPECTED_GLOBAL_QRY_ID_FORMAT;
import static org.apache.ignite.internal.sql.command.SqlKillQueryCommand.parseGlobalQueryId;

/** */
public class KillSqlCommandArg extends IgniteDataTransferObject {
    /** */
    private static final long serialVersionUID = 0;

    /** */
    @Order(value = 0)
    @Positional
    @Argument(description = "Query identifier")
    String queryId;

    /** */
    @Order(value = 1)
    UUID nodeId;

    /** */
    @Order(value = 2)
    long qryId;

    /** */
    public UUID nodeId() {
        return nodeId;
    }

    /** */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /** */
    public long qryId() {
        return qryId;
    }

    /** */
    public void qryId(long qryId) {
        this.qryId = qryId;
    }

    /** */
    public String queryId() {
        return queryId;
    }

    /** */
    public void queryId(String queryId) {
        this.queryId = queryId;

        T2<UUID, Long> ids = parseGlobalQueryId(queryId);

        if (ids == null)
            throw new IllegalArgumentException("Expected global query id. " + EXPECTED_GLOBAL_QRY_ID_FORMAT);

        this.nodeId = ids.get1();
        this.qryId = ids.get2();
    }
}
