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

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertNotContains;

/**
 * Checks command line metadata commands.
 */
public class GridCommandHandlerMetadataTest extends GridCommandHandlerClusterByClassAbstractTest {
    /** File system. */
    public static final FileSystem FS = FileSystems.getDefault();

    /** Types count. */
    private static final int TYPES_CNT = 10;

    /** */
    @Before
    public void init() {
        injectTestSystemOut();
    }

    /** */
    @After
    public void clear() {
        crd.binary().types().stream().forEach(type -> crd.context().cacheObjects().removeType(type.typeId()));
    }

    /**
     * Check the command '--meta list'.
     * Steps:
     * - Creates binary types for a test (by BinaryObjectBuilder);
     * - execute the command '--meta list'.
     * - Check command output (must contains all created types).
     */
    @Test
    public void testMetadataList() {
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
     * Checks that command remove when provided with absent type name prints informative message and
     * returns corresponding error code (invalid argument).
     *
     * Test scenario:
     * <ol>
     *     <li>
     *         Execute meta --remove command passing non-existing type name.
     *     </li>
     *     <li>
     *         Verify that invalid argument error code is returned, message is printed to command output.
     *     </li>
     *     <li>
     *         Execute meta --remove command passing non-existing type id.
     *     </li>
     *     <li>
     *         Verify that invalid argument error code is returned, message is printed to command output.
     *     </li>
     * </ol>
     */
    @Test
    public void testMetadataRemoveWrongType() {
        String wrongTypeName = "Type01";

        assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute("--meta", "remove", "--typeName", wrongTypeName));

        assertContains(log, testOut.toString(), "Failed to remove binary type, type not found: " + wrongTypeName);

        int wrongTypeId = 42;

        assertEquals(EXIT_CODE_INVALID_ARGUMENTS, execute("--meta", "remove", "--typeId", Integer.toString(wrongTypeId)));

        assertContains(log, testOut.toString(), "Failed to remove binary type, type not found: ");
        assertContains(log, testOut.toString(), "0x" +
            Integer.toHexString(wrongTypeId).toUpperCase() + " (" + wrongTypeId + ")");
    }

    /**
     * Checks that commands --meta list and --meta remove don't cause registering system or internal classes
     * in binary metadata.
     *
     * Test scenario:
     * <ol>
     *     <li>
     *         Put a value of a user class to the default cache that requires registering new binary type for that class.
     *     </li>
     *     <li>
     *         Run --meta list command to check that class is registered.
     *     </li>
     *     <li>
     *         Destroy the cache. Remove binary type with --meta remove command.
     *     </li>
     *     <li>
     *         Run --meta list again to check that no additional system or internal classes were registered.
     *     </li>
     * </ol>
     */
    @Test
    public void testMetadataForInternalClassesIsNotRegistered() {
        IgniteCache<Object, Object> dfltCache = grid(0).getOrCreateCache(DEFAULT_CACHE_NAME);

        dfltCache.put(1, new TestValue());

        Collection<BinaryType> metadata = crd.context().cacheObjects().metadata();

        assertEquals(EXIT_CODE_OK, execute("--meta", "list"));

        assertEquals(metadata.toString(), 1, metadata.size());

        assertContains(log, testOut.toString(), "typeName=" + TestValue.class.getTypeName());

        grid(0).destroyCache(DEFAULT_CACHE_NAME);

        metadata = crd.context().cacheObjects().metadata();

        assertEquals(metadata.toString(), 1, metadata.size());

        assertEquals(EXIT_CODE_OK, execute("--meta", "remove", "--typeName", TestValue.class.getTypeName()));

        metadata = crd.context().cacheObjects().metadata();

        assertEquals("Binary metadata is expected to be empty but the following binary types were found: "
            + metadata
                .stream()
                .map(b -> "BinaryType[typeId=" + b.typeId() + ", typeName=" + b.typeName() + ']')
                .collect(Collectors.toList()).toString(),
            0,
            metadata.size());
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
        Path typeFile = FS.getPath("type0.bin");

        try (Connection conn = DriverManager.getConnection(jdbcThinUrl())) {
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
     * Check the type successfully merged after remove-recreate-update operations.
     * Steps:
     * - creates several types with name 'TypeNameX' where X - some index.
     * - removes the type by cmdline utility (store removed metadata to specified file).
     * - checks that type removed.
     * - creates new types with the same names but different field names
     * - restores removed types from the file
     * - checks all types successfully merged.
     */
    @Test
    public void testTypeMergedAfterRemoveUpdate() {
        String[] typeNames = new String[]{"TypeName0", "TypeName1"};

        final int cnt = typeNames.length;

        Object[] typeValues = new Object[]{0, LocalDate.now()};

        int[] typeIds = new int[cnt];

        repeat(cnt, i -> typeIds[i] = crd.binary().builder(typeNames[i])
            .setField("fld0", typeValues[i])
            .build().type().typeId());

        Path[] typeBackups = new Path[typeNames.length];

        try {
            repeat(cnt, i -> {
                Path path = FS.getPath(typeNames[i] + ".bin");

                typeBackups[i] = path;

                assertEquals(EXIT_CODE_OK, execute("--meta", "remove",
                    "--typeName", typeNames[i],
                    "--out", path.toString()));
            });

            assertEquals(EXIT_CODE_OK, execute("--meta", "list"));

            String out = testOut.toString();

            repeat(cnt, i -> assertNotContains(log, out, "typeName=" + typeNames[i]));

            repeat(cnt, i -> crd.binary().builder(typeNames[i])
                .setField("fld1", typeValues[i])
                .build());

            repeat(cnt, i -> assertEquals(EXIT_CODE_OK, execute("--meta", "update", "--in",
                typeBackups[i].toString())));

            repeat(cnt, i -> {
                assertEquals(EXIT_CODE_OK, execute("--meta", "details", "--typeName", typeNames[i]));
                checkTypeDetails(log, testOut.toString(), crd.context().cacheObjects().metadata(typeIds[i]));
            });
        }
        finally {
            repeat(cnt, i -> {
                if (typeBackups[i] != null) {
                    try {
                        Files.deleteIfExists(typeBackups[i]);
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
    }

    /**
     * Check the type can't be merged after remove-recreate-update operations
     * with incompatible changes.
     *
     * Steps:
     * - creates several types with name 'TypeNameX' where X - some index.
     * - removes the type by cmdline utility (store removed metadata to specified file).
     * - checks that type removed.
     * - creates new types with the same names and same fields but different field types.
     * - try to restores and verifies that it fails.
     */
    @Test
    public void testTypeCantBeMergedAfterRemoveUpdateWithIncompatibleChanges() {
        String[] typeNames = new String[]{"TypeName0", "TypeName1"};

        final int cnt = typeNames.length;

        Object[] typeValues = new Object[]{0, LocalDate.now()};

        repeat(cnt, i -> crd.binary().builder(typeNames[i])
            .setField("fld0", typeValues[i])
            .build().type().typeId());

        Path[] typeBackups = new Path[typeNames.length];

        try {
            repeat(cnt, i -> {
                Path path = FS.getPath(typeNames[i] + ".bin");

                typeBackups[i] = path;

                assertEquals(EXIT_CODE_OK, execute("--meta", "remove",
                    "--typeName", typeNames[i],
                    "--out", path.toString()));
            });

            assertEquals(EXIT_CODE_OK, execute("--meta", "list"));

            String out = testOut.toString();

            repeat(cnt, i -> assertNotContains(log, out, "typeName=" + typeNames[i]));

            repeat(cnt, i -> crd.binary().builder(typeNames[i])
                .setField("fld0", typeValues[cnt - i - 1]) // swap values
                .build());

            repeat(cnt, i -> {
                assertEquals(EXIT_CODE_UNEXPECTED_ERROR, execute("--meta", "update", "--in",
                    typeBackups[i].toString()));

                assertContains(log, testOut.toString(), "The type of an existing field can not be changed");
            });
        }
        finally {
            repeat(cnt, i -> {
                if (typeBackups[i] != null) {
                    try {
                        Files.deleteIfExists(typeBackups[i]);
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
    }

    /**
     * Check that you could remove and update type when metadata store under load.
     *
     * Steps:
     * - creates type with name 'Type0'.
     * - start another thread which generates in an infinite loop new types.
     * - removes the type by cmdline utility (store removed metadata to specified file).
     * - checks that type removed.
     * - restore the type.
     * - checks that type restored.
     */
    @Test
    public void testRemoveUpdateUnderLoad() throws Exception {
        Path typeFile = FS.getPath("type0.bin");

        try {
            createType("Type0", 0);

            assertEquals(EXIT_CODE_OK, execute("--meta", "list"));
            assertContains(log, testOut.toString(), "typeName=Type0");

            AtomicBoolean stop = new AtomicBoolean(false);

            Thread t = new Thread(() -> {
                long i = 1;

                while (!stop.get())
                    createType("Type" + i++, i);
            });
            t.start();

            assertEquals(EXIT_CODE_OK, execute("--meta", "remove",
                "--typeName", "Type0",
                "--out", typeFile.toString()));

            assertEquals(EXIT_CODE_OK, execute("--meta", "list"));
            assertNotContains(log, testOut.toString(), "typeName=Type0");

            // Restore the metadata from file.
            assertEquals(EXIT_CODE_OK, execute("--meta", "update", "--in", typeFile.toString()));

            assertEquals(EXIT_CODE_OK, execute("--meta", "list"));
            assertContains(log, testOut.toString(), "typeName=Type0");

            stop.set(true);

            t.join(getTestTimeout());
        }
        finally {
            if (Files.exists(typeFile))
                Files.delete(typeFile);
        }
    }

    /**
     * Checks metadata list/details behaviour after a type removing.
     *
     * Steps:
     * - creates some type.
     * - checks that metadata list|details command returns proper type information.
     * - removes the type by cmdline utility.
     * - checks list command output not contains removed type.
     * - checks details command fails with "Type not found" error.
     */
    @Test
    public void testMetadataListDetailsAfterTypeRemoving() throws IOException {
        Path typeFile = FS.getPath("type0.bin");

        try {
            int typeId = createType("Type0", 0);

            assertEquals(EXIT_CODE_OK, execute("--meta", "list"));
            assertContains(log, testOut.toString(), "typeName=Type0");

            assertEquals(EXIT_CODE_OK, execute("--meta", "details", "--typeName", "Type0"));
            checkTypeDetails(log, testOut.toString(), crd.binary().type(typeId));

            assertEquals(EXIT_CODE_OK, execute("--meta", "remove",
                "--typeName", "Type0",
                "--out", typeFile.toString()));

            assertEquals(EXIT_CODE_OK, execute("--meta", "list"));
            assertNotContains(log, testOut.toString(), "typeName=Type0");

            assertEquals(EXIT_CODE_UNEXPECTED_ERROR, execute("--meta", "details", "--typeName", "Type0"));
            assertContains(log, testOut.toString(), "type not found: " + typeId);
        }
        finally {
            Files.deleteIfExists(typeFile);
        }
    }

    /**
     * Repeats {@code cons} {@code cnt} times.
     *
     * @param cnt Count.
     * @param cons Cons.
     */
    private void repeat(int cnt, Consumer<Integer> cons) {
        for (int i = 0; i < cnt; i++)
            cons.accept(i);
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
    int createType(String typeName, Object val) {
        return createType(crd.binary(), typeName, val);
    }

    /**
     * @param typeName Type name.
     * @param val Field value.
     */
    int createType(IgniteBinary bin, String typeName, Object val) {
        return bin.builder(typeName)
            .setField("fld", val)
            .build()
            .type()
            .typeId();
    }

    /** */
    protected ClientConfiguration clientConfiguration() {
        return new ClientConfiguration()
            .setAddresses("127.0.0.1:10800");
    }

    /** */
    protected String jdbcThinUrl() {
        return "jdbc:ignite:thin://127.0.0.1";
    }

    /**
     *
     */
    public static class TestValue {
        /** */
        public final int val = 3;
    }
}
