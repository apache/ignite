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
package org.apache.ignite.internal.processors.query.calcite.sql;

import java.util.UUID;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDdl;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.sql.kill.IgniteSqlKillComputeTask;
import org.apache.ignite.internal.processors.query.calcite.sql.kill.IgniteSqlKillContinuousQuery;
import org.apache.ignite.internal.processors.query.calcite.sql.kill.IgniteSqlKillQuery;
import org.apache.ignite.internal.processors.query.calcite.sql.kill.IgniteSqlKillScanQuery;
import org.apache.ignite.internal.processors.query.calcite.sql.kill.IgniteSqlKillService;
import org.apache.ignite.internal.processors.query.calcite.sql.kill.IgniteSqlKillTransaction;

/**
 * Abstract base class for KILL queries.
 */
public abstract class IgniteSqlKill extends SqlDdl {
    /** */
    protected IgniteSqlKill(SqlOperator operator, SqlParserPos pos) {
        super(operator, pos);
    }

    /** */
    public static IgniteSqlKill createScanQueryKill(
        SqlParserPos pos,
        SqlCharStringLiteral nodeId,
        SqlCharStringLiteral cacheName,
        SqlNumericLiteral qryId
    ) {
        return new IgniteSqlKillScanQuery(pos, nodeId, cacheName, qryId);
    }

    /** */
    public static IgniteSqlKill createContinuousQueryKill(
        SqlParserPos pos,
        SqlCharStringLiteral nodeId,
        SqlCharStringLiteral routineId
    ) {
        return new IgniteSqlKillContinuousQuery(pos, nodeId, routineId);
    }

    /** */
    public static IgniteSqlKill createServiceKill(
        SqlParserPos pos,
        SqlCharStringLiteral srvName
    ) {
        return new IgniteSqlKillService(pos, srvName);
    }

    /** */
    public static IgniteSqlKill createTransactionKill(
        SqlParserPos pos,
        SqlCharStringLiteral xid
    ) {
        return new IgniteSqlKillTransaction(pos, xid);
    }

    /** */
    public static IgniteSqlKill createComputeTaskKill(
        SqlParserPos pos,
        SqlCharStringLiteral sesId
    ) {
        return new IgniteSqlKillComputeTask(pos, sesId);
    }

    /** */
    public static IgniteSqlKill createQueryKill(
        SqlParserPos pos,
        SqlCharStringLiteral globalQueryId,
        UUID nodeId,
        long queryId,
        boolean isAsync
    ) {
        return new IgniteSqlKillQuery(pos, globalQueryId, nodeId, queryId, isAsync);
    }

    /** */
    public static Pair<UUID, Long> parseGlobalQueryId(String globalQryId) {
        String[] ids = globalQryId.split("_");

        if (ids.length != 2)
            return null;

        try {
            return Pair.of(UUID.fromString(ids[0]), Long.parseLong(ids[1]));
        }
        catch (Exception ignore) {
            return null;
        }
    }
}
