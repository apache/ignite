/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 */
public class FileOutputSignalHandlerTest {

    @Test
    public void testGetOutputFileWithEmptyPath() throws IOException {
        final File f = getOutputFile("", "test1.log");
        assertTrue(f.exists());
        IgniteUtils.deleteIfExists(f.toPath());
    }

    @Test
    public void testGetOutputFileWithPath() throws IOException {
        final String path = "abc";
        final File f = getOutputFile(path, "test2.log");
        assertTrue(f.exists());
        IgniteUtils.deleteIfExists(Paths.get(path));
    }

    @Test
    public void testGetOutputFileWithAbsolutePath() throws IOException {
        final Path path = Paths.get("cde").toAbsolutePath();
        final File f = getOutputFile(path.toString(), "test3.log");
        assertTrue(f.exists());
        IgniteUtils.deleteIfExists(path);
    }

    private File getOutputFile(final String path, final String baseName) throws IOException {
        return new FileOutputSignalHandler() {
            @Override
            public void handle(String signalName) {
            }
        }.getOutputFile(path, baseName);
    }
}
