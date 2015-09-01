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

package org.apache.ignite.schema.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import junit.framework.TestCase;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.schema.model.PojoDescriptor;
import org.apache.ignite.schema.parser.DatabaseMetadataParser;
import org.apache.ignite.schema.ui.ConfirmCallable;
import org.apache.ignite.schema.ui.MessageBox;

import static org.apache.ignite.schema.ui.MessageBox.Result.YES_TO_ALL;

/**
 * Base functional for Ignite Schema Import utility tests.
 */
public abstract class AbstractSchemaImportTest extends TestCase {
    /** DB connection URL. */
    private static final String CONN_URL = "jdbc:h2:mem:autoCacheStore;DB_CLOSE_DELAY=-1";

    /** Path to temp folder where generated POJOs will be saved. */
    protected static final String OUT_DIR_PATH = System.getProperty("java.io.tmpdir") + "/ignite-schema-import/out";

    /** Auto confirmation of file conflicts. */
    protected ConfirmCallable askOverwrite = new ConfirmCallable(null, "") {
        @Override public MessageBox.Result confirm(String msg) {
            return YES_TO_ALL;
        }
    };

    /** List of generated for test database POJO objects. */
    protected List<PojoDescriptor> pojos;

    /** {@inheritDoc} */
    @Override public void setUp() throws Exception {
        Class.forName("org.h2.Driver");

        Connection conn = DriverManager.getConnection(CONN_URL, "sa", "");

        Statement stmt = conn.createStatement();

        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS PRIMITIVES (pk INTEGER PRIMARY KEY, " +
            " boolCol BOOLEAN NOT NULL," +
            " byteCol TINYINT NOT NULL," +
            " shortCol SMALLINT NOT NULL," +
            " intCol INTEGER NOT NULL, " +
            " longCol BIGINT NOT NULL," +
            " floatCol REAL NOT NULL," +
            " doubleCol DOUBLE NOT NULL," +
            " doubleCol2 DOUBLE NOT NULL, " +
            " bigDecimalCol DECIMAL(10, 0)," +
            " strCol VARCHAR(10)," +
            " dateCol DATE," +
            " timeCol TIME," +
            " tsCol TIMESTAMP, " +
            " arrCol BINARY(10))");

        stmt.executeUpdate("CREATE TABLE IF NOT EXISTS OBJECTS (pk INTEGER PRIMARY KEY, " +
            " boolCol BOOLEAN," +
            " byteCol TINYINT," +
            " shortCol SMALLINT," +
            " intCol INTEGER," +
            " longCol BIGINT," +
            " floatCol REAL," +
            " doubleCol DOUBLE," +
            " doubleCol2 DOUBLE," +
            " bigDecimalCol DECIMAL(10, 0)," +
            " strCol VARCHAR(10), " +
            " dateCol DATE," +
            " timeCol TIME," +
            " tsCol TIMESTAMP," +
            " arrCol BINARY(10))");

        conn.commit();

        U.closeQuiet(stmt);

        List<String> schemas = new ArrayList<>();

        pojos = DatabaseMetadataParser.parse(conn, schemas, false);

        U.closeQuiet(conn);
    }

    /**
     * Compare files by lines.
     *
     * @param exp Stream to read of expected file from test resources.
     * @param generated Generated file instance.
     * @param excludePtrn Marker string to exclude lines from comparing.
     * @return true if generated file correspond to expected.
     */
    protected boolean compareFilesInt(InputStream exp, File generated, String excludePtrn) {
        try (BufferedReader baseReader = new BufferedReader(new InputStreamReader(exp))) {
            try (BufferedReader generatedReader = new BufferedReader(new FileReader(generated))) {
                String baseLine;

                while ((baseLine = baseReader.readLine()) != null) {
                    String generatedLine = generatedReader.readLine();

                    if (!baseLine.equals(generatedLine) && !baseLine.contains(excludePtrn)
                            && !generatedLine.contains(excludePtrn)) {
                        System.out.println("Expected: " + baseLine);
                        System.out.println("Generated: " + generatedLine);

                        return false;
                    }
                }

                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();

            return false;
        }
    }
}