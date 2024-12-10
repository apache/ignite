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
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Test client application that either insert or select the BLOB via the Thin JDBC driver.
 */
public class JdbcThinBlobTestApplication extends IgniteAwareApplication {
    /** {@inheritDoc} */
    @Override public void run(JsonNode jsonNode) throws ClassNotFoundException, SQLException, IOException {
        int blobSize = Optional.ofNullable(jsonNode.get("blob_size")).map(JsonNode::asInt).orElse(1);
        String action = Optional.ofNullable(jsonNode.get("action")).map(JsonNode::asText).orElse("insert");
        String mode = Optional.ofNullable(jsonNode.get("mode")).map(JsonNode::asText).orElse("blob");

        try (Connection conn = thinJdbcDataSource.getConnection()) {
            markInitialized();

            conn.createStatement().execute("CREATE TABLE IF NOT EXISTS query(id INT, blob BINARY, PRIMARY KEY(id)) " +
                "WITH \"cache_name=query,template=WITH_STATISTICS_ENABLED\"");

            if ("insert".equals(action)) {
                try (PreparedStatement insStmt = conn.prepareStatement("INSERT INTO query(id, blob) VALUES(?, ?)")) {
                    insStmt.setInt(1, 1);

                    if ("blob".equals(mode)) {
                        Blob blob = conn.createBlob();

                        insStmt.setBlob(2, blob);

                        copyStream(getRandomStream(), blob.setBinaryStream(1), blobSize);
                    }
                    else
                        insStmt.setBlob(2, getRandomStream(), blobSize);

                    insStmt.execute();
                }
            }
            else if ("select".equals(action)) {
                try (PreparedStatement selectStmt = conn.prepareStatement("SELECT * FROM query WHERE id = ?")) {

                    selectStmt.setInt(1, 1);

                    try (ResultSet resultSet = selectStmt.executeQuery()) {
                        while (resultSet.next()) {
                            Blob blob = resultSet.getBlob("blob");

                            if (blob != null)
                                recordResult("BLOB_SIZE", blob.length());
                        }
                    }
                }
            }

            log.info("IGNITE_LOB_APPLICATION_DONE");

            while (!terminated()) {
                try {
                    U.sleep(100); // Keeping node alive.
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

    /** */
    private InputStream getRandomStream() throws IOException {
        return Files.newInputStream(Path.of("/", "dev", "urandom"));
    }

    /** */
    private int copyStream(InputStream in, OutputStream out, long limit) throws IOException {
        int readLen, writtenLen = 0;

        byte[] buf = new byte[1024 * 1024];

        while (-1 != (readLen = in.read(buf, 0, (int)Math.min(buf.length, limit - writtenLen))) && writtenLen < limit) {
            out.write(buf, 0, readLen);

            writtenLen += readLen;
        }

        return writtenLen;
    }
}
