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

package org.apache.ignite.internal.ducktest.tests.sql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

import java.io.IOException;
import java.sql.*;
import java.util.Arrays;
import java.util.Base64;

/**
 *
 */
public class SqlUtilApplication extends IgniteAwareApplication {

    private static final String NL = System.getProperty("line.separator");

    public static void main(String... args) throws IOException, ClassNotFoundException {
        // Register JDBC driver.
        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");

        String[] params = args[0].split(",");

        System.out.println("OUT_START");
        System.out.println(Arrays.toString(args));
        System.err.println("ERR_START");

        ObjectMapper mapper = new ObjectMapper();

        JsonNode jsonNode = mapper.readTree(Base64.getDecoder().decode(params[0]));

        String host = jsonNode.get("host").asText();
        String sql = jsonNode.get("sql").asText();

        log.info(host);
        log.info(sql);

        String url = "jdbc:ignite:thin://" + host + "/?distributedJoins=true";

        SqlUtilApplication app = new SqlUtilApplication();

        long start = 0L;

        try (Connection conn = DriverManager.getConnection(url)){

            app.markInitialized();

            start = System.currentTimeMillis();

            String res = execute(conn, sql);

            for (String line: res.split(NL)) {
                app.recordResult("RESULT", line);
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
            app.recordResult("SQLException", e.toString());
        }

        app.recordResult("DURATION", System.currentTimeMillis() - start);

        app.markFinished();
    }

    @Override
    protected void run(JsonNode jsonNode) throws Exception {

    }

    /**
     * @param conn Connection.
     */
    private static String execute(Connection conn, String sql) throws SQLException {
        Statement stmt = conn.createStatement();

        String res = "";

        if (!stmt.execute(sql))
            return res;

        ResultSet rsltSt = stmt.getResultSet();
        ResultSetMetaData mtDt = rsltSt.getMetaData();
        int colCnt = mtDt.getColumnCount();

        StringBuilder sb = new StringBuilder();

        for (int i = 1; i <= colCnt; i++) {
            sb.append(mtDt.getColumnLabel(i))
                    .append(";");
        }
        sb.append(NL);

        while (rsltSt.next()) {
            for (int i = 1; i <= colCnt; i++) {
                sb.append(rsltSt.getObject(i).toString());
                sb.append(";");
            }

            sb.append(NL);
        }

        res = sb.toString();

        log.info(" - - - RESULT - - - ");
        log.info(res);
        log.info(" - - - END - - - ");

        return res;
    }
}
