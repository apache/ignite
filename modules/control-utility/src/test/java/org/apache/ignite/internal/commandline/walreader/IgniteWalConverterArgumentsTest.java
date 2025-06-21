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

package org.apache.ignite.internal.commandline.walreader;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.stream.LongStream;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.util.lang.ConsumerX;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.util.GridCommandHandlerAbstractTest;
import org.junit.Test;

import static java.nio.charset.Charset.defaultCharset;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.commandline.walreader.IgniteWalConverter.*;
import static org.apache.ignite.internal.commandline.walreader.IgniteWalConverter.validateRecordTypes;
import static org.apache.ignite.internal.processors.diagnostic.DiagnosticProcessor.corruptedPagesFile;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Test for IgniteWalConverterArguments
 */
public class IgniteWalConverterArgumentsTest extends GridCommandHandlerAbstractTest {
    /**
     * Checking whether field "recordTypes" are incorrect.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testIncorrectRecordTypes() throws Exception {
        assertThrows(log, () -> {
            validateRecordTypes("not_exist");
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
            validateRecordTypes("not_exist1,not_exist2");
        }, IllegalArgumentException.class, "Unknown record types: [not_exist1, not_exist2].");
    }

    /**
     * Checking the correctness of the method {@link IgniteWalConverter#parsePageId}.
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
     * Checking the correctness of the method {@link IgniteWalConverter#parsePageIds(File)}.
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
        } finally {
            assertTrue(U.delete(f));
        }
    }

    /**
     * Checking the correctness of the method {@link IgniteWalConverter#parsePageIds(String...)}.
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
    public void testParsePagesArgument() throws Exception {
        withFileTree(ft -> {
            assertTrue(ft.wal().exists());

            assertThrows(log, () ->  collectPages("1"), IllegalArgumentException.class, null);
            assertThrows(log, () -> collectPages( ""), IllegalArgumentException.class, null);

            assertEqualsCollections(F.asList(new T2<>(1, 1L)), collectPages("1:1"));

            File f = new File(System.getProperty("java.io.tmpdir"), "test");

            try {
                Collection<T2<Integer, Long>> pages = collectPages(f.getAbsolutePath());

                assertThrows(log, () -> pages, IllegalArgumentException.class, null);

                assertTrue(f.createNewFile());

                U.writeStringToFile(f, "1:1", defaultCharset().toString(), false);
                assertEqualsCollections(F.asList(new T2<>(1, 1L)), pages);
            } finally {
                assertTrue(U.delete(f));
            }
        });
    }

    /**
     * Checks that the file generated by the diagnostics is correct for the "pages" argument.
     *
     * @throws IOException If failed.
     */
    @Test
    public void testCorruptedPagesFile() throws Exception {
        withFileTree(ft -> {
            int grpId = 10;
            long[] pageIds = {20, 40};

            File f = corruptedPagesFile(ft.root().toPath(), new RandomAccessFileIOFactory(), grpId, pageIds);

            assertTrue(f.exists());
            assertTrue(f.isFile());
            assertTrue(f.length() > 0);

            Collection<T2<Integer, Long>> pages = collectPages(f.getAbsolutePath());

            assertNotNull(pages);

            assertEqualsCollections(
                    LongStream.of(pageIds).mapToObj(pageId -> new T2<>(grpId, pageId)).collect(toList()),
                    pages
            );
        });
    }

    /**
     *
     */
    private void withFileTree(ConsumerX<NodeFileTree> check) throws Exception {
        NodeFileTree ft = nodeFileTree("test");

        ft.wal().mkdirs();
        ft.binaryMeta().mkdirs();

        try {
            check.accept(ft);
        } finally {
            U.delete(ft.root());
        }
    }
}
