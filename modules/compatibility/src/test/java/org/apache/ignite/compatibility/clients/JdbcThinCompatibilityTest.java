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

package org.apache.ignite.compatibility.clients;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.CommonUtils;
import org.apache.ignite.lang.IgniteProductVersion;
import org.junit.Assume;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests JDBC thin compatibility.
 */
@RunWith(Parameterized.class)
public class JdbcThinCompatibilityTest extends AbstractClientCompatibilityTest {
    /** Table name. */
    private static final String TABLE_NAME = "test_table";

    /** URL. */
    private static final String URL = "jdbc:ignite:thin://127.0.0.1";

    /** Rows count. */
    private static final int ROWS_CNT = 10;

    /** Execute sql. */
    private static void executeSql(IgniteEx igniteEx, String sql) {
        igniteEx.context().query().querySqlFields(new SqlFieldsQuery(sql), false).getAll();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        int majorJavaVer = CommonUtils.majorJavaVersion(CommonUtils.jdkVersion());

        if (majorJavaVer > 11) {
            Assume.assumeTrue("Skipped on jdk " + CommonUtils.jdkVersion(),
                VER_2_12_0.compareTo(IgniteProductVersion.fromString(verFormatted)) < 0);
        }
    }

    /** {@inheritDoc} */
    @Override protected void initNode(Ignite ignite) {
        IgniteEx igniteEx = (IgniteEx)ignite;

        executeSql(igniteEx, "CREATE TABLE " + TABLE_NAME + " (id int primary key, name varchar)");

        for (int i = 0; i < ROWS_CNT; i++)
            executeSql(igniteEx, "INSERT INTO " + TABLE_NAME + " (id, name) VALUES(" + i + ", 'name" + i + "')");
    }

    /** {@inheritDoc} */
    @Override protected void testClient(IgniteProductVersion clientVer, IgniteProductVersion serverVer) throws Exception {
        try (Connection conn = DriverManager.getConnection(URL); Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT id, name FROM " + TABLE_NAME + " ORDER BY id");

            assertNotNull(rs);

            int cnt = 0;

            while (rs.next()) {
                int id = rs.getInt("id");
                String name = rs.getString("name");

                assertEquals(cnt, id);
                assertEquals("name" + cnt, name);

                cnt++;
            }

            assertEquals(ROWS_CNT, cnt);
        }
    }
}
