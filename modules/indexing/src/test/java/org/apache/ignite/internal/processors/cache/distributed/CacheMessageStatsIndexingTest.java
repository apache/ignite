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

package org.apache.ignite.internal.processors.cache.distributed;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;

/**
 *
 */
public class CacheMessageStatsIndexingTest extends CacheMessageStatsTest {
    /** URL. */
    private static final String URL = "jdbc:ignite://127.0.0.1/default0";

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(int idx) {
        CacheConfiguration ccfg = super.cacheConfiguration(idx);

        ccfg.setIndexedTypes(Integer.class, Integer.class);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void moreOperations() throws Exception {
        grid("client").cache(DEFAULT_CACHE_NAME + 0).query(new SqlQuery<>(Integer.class, "1=1")).getAll();

        grid("client").cache(DEFAULT_CACHE_NAME + 0).query(new SqlFieldsQuery("select * from Integer where _KEY>? and _KEY<?").setArgs(1, 2)).getAll();

        grid(0).context().io().dumpProcessedMessagesStats();

        Connection conn = DriverManager.getConnection(URL);

        PreparedStatement prepStmt = conn.prepareStatement("select * from \"default0\".Integer p where p._KEY=?");
        prepStmt.setInt(1, 1);

        ResultSet rs = prepStmt.executeQuery();

        while (rs.next());

        rs.close();
        prepStmt.close();
        conn.close();

        grid(0).context().io().dumpProcessedMessagesStats();
        grid(1).context().io().dumpProcessedMessagesStats();
        grid(2).context().io().dumpProcessedMessagesStats();
    }
}
