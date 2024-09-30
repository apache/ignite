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

package org.apache.ignite.internal.ducktest.tests.jdbc;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Simple application that used in smoke tests
 */
public class JdbcThinLobTestApplication extends IgniteAwareApplication {
    /** {@inheritDoc} */
    @Override public void run(JsonNode jsonNode) throws ClassNotFoundException, SQLException, IOException {
        int blobSize = Optional.ofNullable(jsonNode.get("blob_size")).map(JsonNode::asInt).orElse(1);
        int clobSize = Optional.ofNullable(jsonNode.get("clob_size")).map(JsonNode::asInt).orElse(1);
        String action = Optional.ofNullable(jsonNode.get("action")).map(JsonNode::asText).orElse("insert");

        int id = 1;

        try (Connection conn = thinJdbcDataSource.getConnection()) {
            markInitialized();

            conn.createStatement().execute("CREATE TABLE IF NOT EXISTS query(id INT, clob VARCHAR, blob BINARY, PRIMARY KEY(id)) " +
                    "WITH \"cache_name=query,template=WITH_STATISTICS_ENABLED\"");

            if ("insertBlob".equals(action)) {
                PreparedStatement insertStatement = conn.prepareStatement("INSERT INTO query(id, clob, blob) VALUES(?, ?, ?)");

                insertStatement.setInt(1, id);

                Clob clob = getClob(conn, clobSize);
                insertStatement.setClob(2, clob);

                Blob blob = getBlob(conn, blobSize);
                insertStatement.setBlob(3, blob);

                insertStatement.execute();

                insertStatement.close();
            }
            else if ("insertStream".equals(action)) {
                PreparedStatement insertStatement = conn.prepareStatement("INSERT INTO query(id, blob) VALUES(?, ?)");

                insertStatement.setInt(1, id);

                insertStatement.setBlob(2,
                        Files.newInputStream(Path.of("/", "dev", "random")), blobSize);

                log.info("Before execute");
                insertStatement.execute();
                log.info("After execute");

                insertStatement.close();
            }
            else if ("select".equals(action)) {
                PreparedStatement selectStatement = conn.prepareStatement("SELECT * FROM query WHERE id = ?");

                selectStatement.setInt(1, id);

                ResultSet resultSet = selectStatement.executeQuery();

                while (resultSet.next()) {
                    Clob clob = resultSet.getClob("clob");

                    if (clob != null) {
                        recordResult("CLOB_SIZE", clob.length());
                        recordResult("CLOB", clob.getSubString(1, Math.min((int) clob.length(), 64)));
                    }

                    Blob blob = resultSet.getBlob("blob");

                    if (blob != null) {
                        byte[] bytes = blob.getBytes(1, Math.min((int) blob.length(), 64));

                        recordResult("BLOB_SIZE", blob.length());
                        recordResult("BLOB", U.byteArray2String(bytes, "0x%02X", ",0x%02X"));
                    }
                }

                resultSet.close();
                selectStatement.close();
            }

            recordMemoryPeakUsage();

            log.info("IGNITE_LOB_APPLICATION_DONE");

            while (!terminated()) {
                try {
                    U.sleep(100); // Keeping node/txs alive.
                }
                catch (IgniteInterruptedCheckedException ignored) {
                    log.info("Waiting interrupted.");
                }
            }

            markFinished();
        }
        catch (Exception e) {
            log.error(e);

            throw e;
        }
    }

    /**
     * @param conn Connection.
     * @param size CLOB size.
     * @return CLOB of specified size.
     */
    private Clob getClob(Connection conn, int size) throws SQLException {
        Clob clob = conn.createClob();

        if (size > 0) {
            StringBuilder sb = new StringBuilder(size);

            for (int i = 0; i < size; i++) {
                sb.append("Ж");
            }

            clob.setString(1, sb.toString());
        }
        return clob;
    }

    /**
     * @param conn Connection.
     * @param size BLOB size.
     * @return BLOB with random data.
     */
    private Blob getBlob(Connection conn, int size) throws SQLException {
        Blob blob = conn.createBlob();

        if (size > 0) {
            byte[] bytes = new byte[size];

            new Random().nextBytes(bytes);

            blob.setBytes(1, bytes);
        }

        return blob;
    }

    /**
     * @return Memory usage.
     */
    private MemoryUsage getPeakMemoryUsage() {
        List<MemoryPoolMXBean> pools = ManagementFactory.getMemoryPoolMXBeans();

        return pools.stream().filter(pool -> pool.getName().contains("G1"))
                .map(MemoryPoolMXBean::getPeakUsage)
                .reduce((MemoryUsage a, MemoryUsage b) ->
                        new MemoryUsage(a.getInit() + b.getInit(), a.getUsed() + b.getUsed(),
                                a.getCommitted() + b.getCommitted(),
                                Math.max(a.getMax() + b.getMax(), a.getCommitted() + b.getCommitted())))
                .orElse(null);
    }

    /**
     */
    private void recordMemoryPeakUsage() {
        MemoryUsage peakMemoryUsage = getPeakMemoryUsage();

        recordResult("PEAK_MEMORY_USED", peakMemoryUsage.getUsed());
        recordResult("PEAK_MEMORY_COMMITTED", peakMemoryUsage.getCommitted());
        recordResult("PEAK_MEMORY_MAX", peakMemoryUsage.getMax());
        recordResult("PEAK_MEMORY_INIT", peakMemoryUsage.getInit());
    }
}
