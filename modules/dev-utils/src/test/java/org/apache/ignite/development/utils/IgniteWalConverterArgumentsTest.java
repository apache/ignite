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

package org.apache.ignite.development.utils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.stream.LongStream;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

import static java.nio.charset.Charset.defaultCharset;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.development.utils.IgniteWalConverterArguments.parse;
import static org.apache.ignite.development.utils.IgniteWalConverterArguments.parsePageId;
import static org.apache.ignite.development.utils.IgniteWalConverterArguments.parsePageIds;
import static org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessor.corruptedPagesFile;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Test for IgniteWalConverterArguments
 */
public class IgniteWalConverterArgumentsTest extends GridCommonAbstractTest {
    /**
     *
     */
    public IgniteWalConverterArgumentsTest() {
        super(false);
    }

    /**
     * View help
     * <ul>
     *     <li>Read wal with out params</li>
     * </ul>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testViewHelp() throws Exception {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        final IgniteWalConverterArguments parseArgs = parse(new PrintStream(out), null);

        Assert.assertNull(parseArgs);

        final String help = out.toString();

        Assert.assertTrue(help.startsWith("Print WAL log data in human-readable form."));

        for (final Field field : IgniteWalConverterArguments.class.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers())
                && Modifier.isStatic(field.getModifiers())
                && field.getType() == String.class) {
                field.setAccessible(true);

                final String arg = (String)field.get(null);

                Assert.assertTrue(help.contains("    " + arg + " "));
            }
        }
    }

    /**
     * Checking whether fields "walDir" or "walArchiveDir" are mandatory.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRequiredWalDir() throws Exception {
        assertThrows(log, () -> {
            parse(System.out, new String[] {"pageSize=4096"});
        }, IgniteException.class, "The paths to the WAL files are not specified.");
    }

    /**
     * Checking whether field "walDir" are incorrect.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectWalDir() throws Exception {
        assertThrows(log, () -> {
            parse(System.out, new String[] {"walDir=non_existing_path"});
        }, IgniteException.class, "Incorrect path to dir with wal files: non_existing_path");
    }

    /**
     * Checking whether field "walArchiveDir" are incorrect.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectWalArchiveDir() throws Exception {
        assertThrows(log, () -> {
            parse(System.out, new String[] {"walArchiveDir=non_existing_path"});
        }, IgniteException.class, "Incorrect path to dir with archive wal files: non_existing_path");
    }

    /**
     * Checking whether field "pageSize" are incorrect.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectPageSize() throws Exception {
        assertThrows(log, () -> {
            final File wal = File.createTempFile("wal", "");
            wal.deleteOnExit();

            final String[] args = {
                "walDir=" + wal.getAbsolutePath(),
                "pageSize=not_integer"
            };

            parse(System.out, args);
        }, IgniteException.class, "Incorrect page size. Error parse: not_integer");
    }

    /**
     * Checking whether field "binaryMetadataFileStoreDir" are incorrect.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectBinaryMetadataFileStoreDir() throws Exception {
        assertThrows(log, () -> {
            final File wal = File.createTempFile("wal", "");
            wal.deleteOnExit();

            final String[] args = {
                "walDir=" + wal.getAbsolutePath(),
                "binaryMetadataFileStoreDir=non_existing_path"
            };

            parse(System.out, args);
        }, IgniteException.class, "Incorrect path to dir with binary meta files: non_existing_path");
    }

    /**
     * Checking whether field "marshallerMappingFileStoreDir" are incorrect.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectMarshallerMappingFileStoreDir() throws Exception {
        assertThrows(log, () -> {
            final File wal = File.createTempFile("wal", "");
            wal.deleteOnExit();

            final String[] args = {
                "walDir=" + wal.getAbsolutePath(),
                "marshallerMappingFileStoreDir=non_existing_path"
            };

            parse(System.out, args);
        }, IgniteException.class, "Incorrect path to dir with marshaller files: non_existing_path");
    }

    /**
     * Checking whether field "keepBinary" are incorrect.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectKeepBinary() throws Exception {
        assertThrows(log, () -> {
            final File wal = File.createTempFile("wal", "");
            wal.deleteOnExit();

            final String[] args = {
                "walDir=" + wal.getAbsolutePath(),
                "keepBinary=not_boolean"
            };

            parse(System.out, args);
        }, IgniteException.class, "Incorrect flag keepBinary, valid value: true or false. Error parse: not_boolean");
    }

    /**
     * Checking whether field "recordTypes" are incorrect.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectRecordTypes() throws Exception {
        assertThrows(log, () -> {
            final File wal = File.createTempFile("wal", "");
            wal.deleteOnExit();

            final String[] args = {
                "walDir=" + wal.getAbsolutePath(),
                "recordTypes=not_exist"
            };

            parse(System.out, args);
        }, IgniteException.class, "Unknown record types: [not_exist].");
    }

    /**
     * Checking whether field "recordTypes" are incorrect several value.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectSeveralRecordTypes() throws Exception {
        assertThrows(log, () -> {
            final File wal = File.createTempFile("wal", "");
            wal.deleteOnExit();

            final String[] args = {
                "walDir=" + wal.getAbsolutePath(),
                "recordTypes=not_exist1,not_exist2"
            };

            parse(System.out, args);
        }, IgniteException.class, "Unknown record types: [not_exist1, not_exist2].");
    }

    /**
     * Checking whether field "walTimeFromMillis" are incorrect.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectWalTimeFromMillis() throws Exception {
        assertThrows(log, () -> {
            final File wal = File.createTempFile("wal", "");
            wal.deleteOnExit();

            final String[] args = {
                "walDir=" + wal.getAbsolutePath(),
                "walTimeFromMillis=not_long"
            };

            parse(System.out, args);
        }, IgniteException.class, "Incorrect walTimeFromMillis. Error parse: not_long");
    }

    /**
     * Checking whether field "walTimeToMillis" are incorrect.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectWalTimeToMillis() throws Exception {
        assertThrows(log, () -> {
            final File wal = File.createTempFile("wal", "");
            wal.deleteOnExit();

            final String[] args = {
                "walDir=" + wal.getAbsolutePath(),
                "walTimeToMillis=not_long"
            };

            parse(System.out, args);
        }, IgniteException.class, "Incorrect walTimeToMillis. Error parse: not_long");
    }

    /**
     * Checking whether field "processSensitiveData" are incorrect.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectProcessSensitiveData() throws Exception {
        assertThrows(log, () -> {
            final File wal = File.createTempFile("wal", "");
            wal.deleteOnExit();

            final String[] args = {
                "walDir=" + wal.getAbsolutePath(),
                "processSensitiveData=unknown"
            };

            parse(System.out, args);
        }, IgniteException.class, "Unknown processSensitiveData: unknown. Supported: ");
    }

    /**
     * Checking whether field "printStat" are incorrect.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectPrintStat() throws Exception {
        assertThrows(log, () -> {
            final File wal = File.createTempFile("wal", "");
            wal.deleteOnExit();

            final String[] args = {
                "walDir=" + wal.getAbsolutePath(),
                "printStat=not_boolean"
            };

            parse(System.out, args);
        }, IgniteException.class, "Incorrect flag printStat, valid value: true or false. Error parse: not_boolean");
    }

    /**
     * Checking whether field "skipCrc" are incorrect.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectSkipCrc() throws Exception {
        assertThrows(log, () -> {
            final File wal = File.createTempFile("wal", "");
            wal.deleteOnExit();

            final String[] args = {
                "walDir=" + wal.getAbsolutePath(),
                "skipCrc=not_boolean"
            };

            parse(System.out, args);
        }, IgniteException.class, "Incorrect flag skipCrc, valid value: true or false. Error parse: not_boolean");
    }

    /**
     * Checking default value.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDefault() throws IOException {
        final File wal = File.createTempFile("wal", "");
        wal.deleteOnExit();

        final String[] args = {
            "walDir=" + wal.getAbsolutePath()
        };

        final IgniteWalConverterArguments parseArgs = parse(System.out, args);

        Assert.assertEquals(4096, parseArgs.getPageSize());
        Assert.assertNull(parseArgs.getBinaryMetadataFileStoreDir());
        Assert.assertNull(parseArgs.getMarshallerMappingFileStoreDir());
        Assert.assertTrue(parseArgs.isKeepBinary());
        Assert.assertTrue(parseArgs.getRecordTypes().isEmpty());
        Assert.assertNull(parseArgs.getFromTime());
        Assert.assertNull(parseArgs.getToTime());
        Assert.assertNull(parseArgs.getRecordContainsText());
        Assert.assertEquals(ProcessSensitiveData.SHOW, parseArgs.getProcessSensitiveData());
        Assert.assertFalse(parseArgs.isPrintStat());
        Assert.assertFalse(parseArgs.isSkipCrc());
    }

    /**
     * Checking all value set.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testParse() throws IOException {
        final File wal = File.createTempFile("wal", "");
        wal.deleteOnExit();

        final File walArchive = File.createTempFile("wal_archive", "");
        walArchive.deleteOnExit();

        final File binaryMetadataDir = new File(System.getProperty("java.io.tmpdir"));

        final File marshallerDir = binaryMetadataDir;

        final String[] args = {
            "walDir=" + wal.getAbsolutePath(),
            "walArchiveDir=" + walArchive.getAbsolutePath(),
            "pageSize=2048",
            "binaryMetadataFileStoreDir=" + binaryMetadataDir.getAbsolutePath(),
            "marshallerMappingFileStoreDir=" + marshallerDir.getAbsolutePath(),
            "keepBinary=false",
            "recordTypes=DATA_RECORD_V2,TX_RECORD",
            "walTimeFromMillis=1575158400000",
            "walTimeToMillis=1577836740999",
            "recordContainsText=search string",
            "processSensitiveData=MD5",
            "printStat=true",
            "skipCrc=true"};

        final IgniteWalConverterArguments parseArgs = parse(System.out, args);
        Assert.assertEquals(wal, parseArgs.getWalDir());
        Assert.assertEquals(walArchive, parseArgs.getWalArchiveDir());
        Assert.assertEquals(2048, parseArgs.getPageSize());
        Assert.assertEquals(binaryMetadataDir, parseArgs.getBinaryMetadataFileStoreDir());
        Assert.assertEquals(marshallerDir, parseArgs.getMarshallerMappingFileStoreDir());
        Assert.assertFalse(parseArgs.isKeepBinary());
        Assert.assertTrue(parseArgs.getRecordTypes().contains(WALRecord.RecordType.DATA_RECORD_V2));
        Assert.assertTrue(parseArgs.getRecordTypes().contains(WALRecord.RecordType.TX_RECORD));
        Assert.assertEquals(1575158400000L, (long)parseArgs.getFromTime());
        Assert.assertEquals(1577836740999L, (long)parseArgs.getToTime());
        Assert.assertEquals("search string", parseArgs.getRecordContainsText());
        Assert.assertEquals(ProcessSensitiveData.MD5, parseArgs.getProcessSensitiveData());
        Assert.assertTrue(parseArgs.isPrintStat());
        Assert.assertTrue(parseArgs.isSkipCrc());
    }

    /**
     * Checking the correctness of the method {@link IgniteWalConverterArguments#parsePageId}.
     */
    @Test
    public void testParsePageId() {
        String[] invalidValues = {
            null,
            "",
            " ",
            "a",
            "a:",
            "a:b",
            "a:b",
            "a:1",
            "1:b",
            "1;1",
            "1a:1",
            "1:1b",
            "1:1:1",
        };

        for (String v : invalidValues)
            assertThrows(log, () -> parsePageId(v), IllegalArgumentException.class, null);

        assertEquals(new T2<>(1, 1L), parsePageId("1:1"));
        assertEquals(new T2<>(-1, 1L), parsePageId("-1:1"));
        assertEquals(new T2<>(1, -1L), parsePageId("1:-1"));
        assertEquals(new T2<>(-1, -1L), parsePageId("-1:-1"));
    }

    /**
     * Checking the correctness of the method {@link IgniteWalConverterArguments#parsePageIds(File)}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testParsePageIdsFile() throws Exception {
        File f = new File(System.getProperty("java.io.tmpdir"), "test");

        try {
            assertThrows(log, () -> parsePageIds(f), IllegalArgumentException.class, null);

            assertTrue(f.createNewFile());

            assertTrue(parsePageIds(f).isEmpty());

            U.writeStringToFile(f, "a:b", defaultCharset().toString(), false);
            assertThrows(log, () -> parsePageIds(f), IllegalArgumentException.class, null);

            U.writeStringToFile(f, "1:1,1:1", defaultCharset().toString(), false);
            assertThrows(log, () -> parsePageIds(f), IllegalArgumentException.class, null);

            U.writeStringToFile(f, "1:1", defaultCharset().toString(), false);
            assertEqualsCollections(F.asList(new T2<>(1, 1L)), parsePageIds(f));

            U.writeStringToFile(f, U.nl() + "2:2", defaultCharset().toString(), true);
            assertEqualsCollections(F.asList(new T2<>(1, 1L), new T2<>(2, 2L)), parsePageIds(f));

            U.writeStringToFile(f, U.nl() + "-1:1", defaultCharset().toString(), true);
            U.writeStringToFile(f, U.nl() + "1:-1", defaultCharset().toString(), true);
            U.writeStringToFile(f, U.nl() + "-1:-1", defaultCharset().toString(), true);

            assertEqualsCollections(
                F.asList(new T2<>(1, 1L), new T2<>(2, 2L), new T2<>(-1, 1L), new T2<>(1, -1L), new T2<>(-1, -1L)),
                parsePageIds(f)
            );
        }
        finally {
            assertTrue(U.delete(f));
        }
    }

    /**
     * Checking the correctness of the method {@link IgniteWalConverterArguments#parsePageIds(String...)}.
     */
    @Test
    public void testParsePageIdsStrings() {
        assertTrue(parsePageIds().isEmpty());

        assertThrows(log, () -> parsePageIds("a:b"), IllegalArgumentException.class, null);
        assertThrows(log, () -> parsePageIds("1:1", "a:b"), IllegalArgumentException.class, null);

        assertEqualsCollections(F.asList(new T2<>(1, 1L)), parsePageIds("1:1"));

        assertEqualsCollections(
            F.asList(new T2<>(1, 1L), new T2<>(2, 2L), new T2<>(-1, 1L), new T2<>(1, -1L), new T2<>(-1, -1L)),
            parsePageIds("1:1", "2:2", "-1:1", "1:-1", "-1:-1")
        );
    }

    /**
     * Checking the correctness of parsing the argument "pages".
     *
     * @throws Exception If failed.
     */
    @Test
    public void testParsePagesArgument() throws IOException {
        File walDir = new File(System.getProperty("java.io.tmpdir"), "walDir");

        try {
            assertTrue(walDir.mkdir());

            String walDirStr = "walDir=" + walDir.getAbsolutePath();

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(baos);

            assertThrows(log, () -> parse(ps, walDirStr, "pages=1"), IllegalArgumentException.class, null);
            assertThrows(log, () -> parse(ps, walDirStr, "pages="), IllegalArgumentException.class, null);

            assertEqualsCollections(F.asList(new T2<>(1, 1L)), parse(ps, walDirStr, "pages=1:1").getPages());

            File f = new File(System.getProperty("java.io.tmpdir"), "test");

            try {
                String pagesFileStr = "pages=" + f.getAbsolutePath();

                assertThrows(log, () -> parse(ps, walDirStr, pagesFileStr), IllegalArgumentException.class, null);

                assertTrue(f.createNewFile());
                assertTrue(parse(ps, walDirStr, pagesFileStr).getPages().isEmpty());

                U.writeStringToFile(f, "1:1", defaultCharset().toString(), false);
                assertEqualsCollections(F.asList(new T2<>(1, 1L)), parse(ps, walDirStr, pagesFileStr).getPages());
            }
            finally {
                assertTrue(U.delete(f));
            }
        }
        finally {
            assertTrue(U.delete(walDir));
        }
    }

    /**
     * Checks that the file generated by the diagnostics is correct for the "pages" argument.
     *
     * @throws IOException If failed.
     */
    @Test
    public void testCorruptedPagesFile() throws IOException {
        File tmpDir = new File(System.getProperty("java.io.tmpdir"), getName());

        try {
            int grpId = 10;
            long[] pageIds = {20, 40};

            File f = corruptedPagesFile(tmpDir.toPath(), new RandomAccessFileIOFactory(), grpId, pageIds);

            assertTrue(f.exists());
            assertTrue(f.isFile());
            assertTrue(f.length() > 0);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(baos);

            IgniteWalConverterArguments args =
                parse(ps, "walDir=" + tmpDir.getAbsolutePath(), "pages=" + f.getAbsolutePath());

            assertNotNull(args.getPages());

            assertEqualsCollections(
                LongStream.of(pageIds).mapToObj(pageId -> new T2<>(grpId, pageId)).collect(toList()),
                args.getPages()
            );
        }
        finally {
            if (tmpDir.exists())
                assertTrue(U.delete(tmpDir));
        }
    }
}
