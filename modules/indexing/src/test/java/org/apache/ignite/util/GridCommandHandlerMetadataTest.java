/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.util;

import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.UUID;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.binary.BinarySchema;
import org.apache.ignite.internal.binary.BinaryTypeImpl;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;

/**
 * Checks command line metadata commands.
 */
public class GridCommandHandlerMetadataTest extends GridCommandHandlerClusterByClassAbstractTest {
    /** File system. */
    public static final FileSystem FS = FileSystems.getDefault();

    /** Types count. */
    private static final int TYPES_CNT = 10;

    /**
     * Check the command '--meta list'.
     * Steps:
     * - Creates binary types for a test (by BinaryObjectBuilder);
     * - execute the command '--meta list'.
     * - Check command output (must contains all created types).
     */
    @Test
    public void testMetadataList() {
        injectTestSystemOut();

        for (int typeNum = 0; typeNum < TYPES_CNT; ++typeNum) {
            BinaryObjectBuilder bob = crd.binary().builder("Type_" + typeNum);

            for (int fldNum = 0; fldNum <= typeNum; ++fldNum)
                bob.setField("fld_" + fldNum, 0);

            bob.build();
        }

        assertEquals(EXIT_CODE_OK, execute("--meta", "list"));

        String out = testOut.toString();

        for (int typeNum = 0; typeNum < TYPES_CNT; ++typeNum)
            assertContains(log, out, "typeName=Type_" + typeNum);
    }

    /**
     * Check the command '--meta details'.
     * Steps:
     * - Creates binary two types for a test (by BinaryObjectBuilder) with several fields and shemas;
     * - execute the command '--meta details' for the type Type0 by name
     * - check metadata print.
     * - execute the command '--meta details' for the type Type0 by type ID on different formats.
     * - check metadata print.
     */
    @Test
    public void testMetadataDetails() {
        injectTestSystemOut();

        BinaryObjectBuilder bob0 = crd.binary().builder("TypeName0");
        bob0.setField("fld0", 0);
        bob0.build();

        bob0 = crd.binary().builder("TypeName0");
        bob0.setField("fld1", "0");
        bob0.build();

        bob0 = crd.binary().builder("TypeName0");
        bob0.setField("fld0", 1);
        bob0.setField("fld2", UUID.randomUUID());
        BinaryObject bo0 = bob0.build();

        BinaryObjectBuilder bob1 = crd.binary().builder("TypeName1");
        bob1.setField("fld0", 0);
        bob1.build();

        bob1 = crd.binary().builder("TypeName1");
        bob1.setField("fld0", 0);
        bob1.setField("fld1", new Date());
        bob1.setField("fld2", 0.1);
        bob1.setField("fld3", new long[]{0, 1, 2, 3});

        BinaryObject bo1 = bob1.build();

        assertEquals(EXIT_CODE_OK, execute("--meta", "details", "--typeName", "TypeName0"));
        checkTypeDetails(log, testOut.toString(), crd.context().cacheObjects().metadata(bo0.type().typeId()));

        assertEquals(EXIT_CODE_OK, execute("--meta", "details", "--typeId",
            "0x" + Integer.toHexString(crd.context().cacheObjects().typeId("TypeName1"))));
        checkTypeDetails(log, testOut.toString(), crd.context().cacheObjects().metadata(bo1.type().typeId()));

        assertEquals(EXIT_CODE_OK, execute("--meta", "details", "--typeId",
            Integer.toString(crd.context().cacheObjects().typeId("TypeName1"))));
        checkTypeDetails(log, testOut.toString(), crd.context().cacheObjects().metadata(bo1.type().typeId()));
    }

    /**
     * Check the command '--meta remove' and '--meta update' with invalid arguments.
     * Steps:
     * - executes 'remove' command without specified type.
     * - checks error code and command output.
     * - executes 'remove' command with '--out' option specified as the exist directory
     *      ('--out' parameter must be a file name).
     * - checks error code and command output.
     * - executes 'update' command with '--in' option specified as the exist directory
     *      ('--in' parameter must be a file name)
     * - checks error code and command output.
     */
    @Test
    public void testInvalidArguments() {
        injectTestSystemOut();

        String out;

        assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute("--meta", "remove"));
        out = testOut.toString();
        assertContains(log, out, "Check arguments.");
        assertContains(log, out, "Type to remove is not specified");
        assertContains(log, out, "Please add one of the options: --typeName <type_name> or --typeId <type_id>");

        assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute("--meta", "remove", "--typeId", "0", "--out", "target"));
        out = testOut.toString();
        assertContains(log, out, "Check arguments.");
        assertContains(log, out, "Cannot write to output file target.");

        assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute("--meta", "update", "--in", "target"));
        out = testOut.toString();
        assertContains(log, out, "Check arguments.");
        assertContains(log, out, "Cannot read metadata from target");
    }

    /**
     * Check the command '--meta remove' and '--meta update'.
     * Steps:
     * - creates the type 'Type0'.
     * - removes the type by cmdline utility (store removed metadata to specified file).
     * - checks that type removed (try to create Type0 with the same field and different type).
     * - removes the new type 'Type0' by cmd line utility.
     * - restores Typo0 from the file
     * - checks restored type.
     */
    @Test
    public void testRemoveUpdate() throws Exception {
        injectTestSystemOut();

        Path typeFile = FS.getPath("type0.bin");

        try {
            createType("Type0", 0);

            // Type of the field cannot be changed.
            GridTestUtils.assertThrowsAnyCause(log, () -> {
                createType("Type0", "string");

                return null;
            }, BinaryObjectException.class, "Wrong value has been set");

            assertEquals(EXIT_CODE_OK, execute("--meta", "remove",
                "--typeName", "Type0",
                "--out", typeFile.toString()));

            // New type must be created successfully after remove the type.
            createType("Type0", "string");

            assertEquals(EXIT_CODE_OK, execute("--meta", "remove", "--typeName", "Type0"));

            // Restore the metadata from file.
            assertEquals(EXIT_CODE_OK, execute("--meta", "update", "--in", typeFile.toString()));

            // Type of the field cannot be changed.
            GridTestUtils.assertThrowsAnyCause(log, () -> {
                createType("Type0", "string");

                return null;
            }, BinaryObjectException.class, "Wrong value has been set");

            createType("Type0", 1);

            crd.context().cacheObjects().removeType(crd.context().cacheObjects().typeId("Type0"));

            createType("Type0", "string");

            // Restore the metadata from file.
            assertEquals(EXIT_CODE_UNEXPECTED_ERROR, execute("--meta", "update", "--in", typeFile.toString()));

            String out = testOut.toString();

            assertContains(log, out, "Failed to execute metadata command='update'");
            assertContains(log, out, "Type 'Type0' with typeId 110843958 has a " +
                "different/incorrect type for field 'fld'.");
            assertContains(log, out, "Expected 'String' but 'int' was provided. " +
                "The type of an existing field can not be changed");
        }
        finally {
            if (Files.exists(typeFile))
                Files.delete(typeFile);
        }
    }

    /**
     * Check the all thin connections are dropped on the command '--meta remove' and '--meta update'.
     * Steps:
     * - opens thin client connection.
     * - creates Type0 on thin client side.
     * - removes type by cmd line util.
     * - executes any command on thin client to detect disconnect.
     * - creates Type0 on thin client side with other type of field (checks the type was removed).
     */
    @Test
    public void testDropThinConnectionsOnRemove() throws Exception {
        injectTestSystemOut();

        Path typeFile = FS.getPath("type0.bin");

        try (IgniteClient cli = Ignition.startClient(clientConfiguration())) {
            createType(cli.binary(), "Type0", 1);

            assertEquals(EXIT_CODE_OK, execute("--meta", "remove",
                "--typeName", "Type0",
                "--out", typeFile.toString()));

            // Executes command to check disconnect / reconnect.
            GridTestUtils.assertThrows(log, () ->
                    cli.createCache(new ClientCacheConfiguration().setName("test")),
                Exception.class, null);

            createType(cli.binary(), "Type0", "str");
        }
        finally {
            if (Files.exists(typeFile))
                Files.delete(typeFile);
        }
    }

    /**
     * Check the all thin connections are dropped on the command '--meta remove' and '--meta update'.
     * Steps:
     * - opens JDBC thin client connection.
     * - executes: CREATE TABLE test(id INT PRIMARY KEY, objVal OTHER).
     * - inserts the instance of the 'TestValue' class to the table.
     * - removes the type 'TestValue' by cmd line.
     * - executes any command on JDBC driver to detect disconnect.
     * - checks metadata on client side. It must be empty.
     */
    @Test
    public void testDropJdbcThinConnectionsOnRemove() throws Exception {
        injectTestSystemOut();

        Path typeFile = FS.getPath("type0.bin");

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1")) {
            try (final Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE test(id INT PRIMARY KEY, objVal OTHER)");

                try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO test(id, objVal) VALUES (?, ?)")) {
                    pstmt.setInt(1, 0);
                    pstmt.setObject(2, new TestValue());

                    pstmt.execute();
                }

                stmt.execute("DELETE FROM test WHERE id >= 0");
            }

            HashMap<Integer, BinaryType> metasOld = GridTestUtils.getFieldValue(conn, "metaHnd", "cache", "metas");

            assertFalse(metasOld.isEmpty());

            assertEquals(EXIT_CODE_OK, execute("--meta", "remove",
                "--typeName", TestValue.class.getName(),
                "--out", typeFile.toString()));

            // Executes any command to check disconnect.
            GridTestUtils.assertThrows(log, () -> {
                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute("SELECT * FROM test");
                    }

                    return null;
                },
                SQLException.class, "Failed to communicate with Ignite cluster");

            HashMap<Integer, BinaryType> metas = GridTestUtils.getFieldValue(conn, "metaHnd", "cache", "metas");

            assertNotSame(metasOld, metas);
            assertTrue(metas.isEmpty());
        }
        finally {
            if (Files.exists(typeFile))
                Files.delete(typeFile);
        }
    }

    /**
     * @param t Binary type.
     */
    private void checkTypeDetails(@Nullable IgniteLogger log, String cmdOut, BinaryType t) {
        assertContains(log, cmdOut, "typeId=" + "0x" + Integer.toHexString(t.typeId()).toUpperCase());
        assertContains(log, cmdOut, "typeName=" + t.typeName());
        assertContains(log, cmdOut, "Fields:");

        for (String fldName : t.fieldNames())
            assertContains(log, cmdOut, "name=" + fldName + ", type=" + t.fieldTypeName(fldName));

        for (BinarySchema s : ((BinaryTypeImpl)t).metadata().schemas())
            assertContains(log, cmdOut, "schemaId=0x" + Integer.toHexString(s.schemaId()).toUpperCase());
    }

    /**
     * @param typeName Type name
     * @param val Field value.
     */
    void createType(String typeName, Object val) {
        createType(crd.binary(), typeName, val);
    }

    /**
     * @param typeName Type name.
     * @param val Field value.
     */
    void createType(IgniteBinary bin, String typeName, Object val) {
        BinaryObjectBuilder bob = bin.builder(typeName);
        bob.setField("fld", val);
        bob.build();
    }

    /** */
    private ClientConfiguration clientConfiguration() {
        return new ClientConfiguration()
            .setAddresses("127.0.0.1:10800");
    }

    /**
     *
     */
    public static class TestValue {
        /** */
        public final int val = 3;
    }
}
