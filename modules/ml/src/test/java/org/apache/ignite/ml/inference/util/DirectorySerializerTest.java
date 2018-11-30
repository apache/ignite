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

package org.apache.ignite.ml.inference.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Scanner;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link DirectorySerializer} class.
 */
public class DirectorySerializerTest {
    /** Source directory prefix. */
    private static final String SRC_DIRECTORY_PREFIX = "directory_serializer_test_src";

    /** Destination directory prefix. */
    private static final String DST_DIRECTORY_PREFIX = "directory_serializer_test_dst";

    /** */
    @Test
    public void testSerializeDeserializeWithFile() throws IOException, ClassNotFoundException {
        Path src = Files.createTempDirectory(SRC_DIRECTORY_PREFIX);
        Path dst = Files.createTempDirectory(DST_DIRECTORY_PREFIX);
        try {
            File file = new File(src.toString() + "/test.txt");
            Files.createFile(file.toPath());
            try (FileWriter fw = new FileWriter(file)) {
                fw.write("Hello, world!");
                fw.flush();
            }

            byte[] serialized = DirectorySerializer.serialize(src);
            DirectorySerializer.deserialize(dst, serialized);

            File[] files = dst.toFile().listFiles();

            assertNotNull(files);
            assertEquals(1, files.length);
            assertEquals("test.txt", files[0].getName());

            try (Scanner scanner = new Scanner(files[0])) {
                assertTrue(scanner.hasNextLine());
                assertEquals("Hello, world!", scanner.nextLine());
                assertFalse(scanner.hasNextLine());
            }
        }
        finally {
            DirectorySerializer.deleteDirectory(src);
            DirectorySerializer.deleteDirectory(dst);
        }
    }

    /** */
    @Test
    public void testSerializeDeserializeWithDirectory() throws IOException, ClassNotFoundException {
        Path src = Files.createTempDirectory(SRC_DIRECTORY_PREFIX);
        Path dst = Files.createTempDirectory(DST_DIRECTORY_PREFIX);
        try {
            Files.createDirectories(Paths.get(src.toString() + "/a/b/"));
            File file = new File(src.toString() + "/a/b/test.txt");
            Files.createFile(file.toPath());
            try (FileWriter fw = new FileWriter(file)) {
                fw.write("Hello, world!");
                fw.flush();
            }

            byte[] serialized = DirectorySerializer.serialize(src);
            DirectorySerializer.deserialize(dst, serialized);

            File[] files = dst.toFile().listFiles();

            assertNotNull(files);
            assertEquals(1, files.length);
            assertEquals("a", files[0].getName());
            assertTrue(files[0].isDirectory());

            files = files[0].listFiles();

            assertNotNull(files);
            assertEquals(1, files.length);
            assertEquals("b", files[0].getName());
            assertTrue(files[0].isDirectory());

            files = files[0].listFiles();

            assertNotNull(files);
            assertEquals(1, files.length);
            assertEquals("test.txt", files[0].getName());

            try (Scanner scanner = new Scanner(files[0])) {
                assertTrue(scanner.hasNextLine());
                assertEquals("Hello, world!", scanner.nextLine());
                assertFalse(scanner.hasNextLine());
            }
        }
        finally {
            DirectorySerializer.deleteDirectory(src);
            DirectorySerializer.deleteDirectory(dst);
        }
    }
}
