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

package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.dumpprocessors;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.SharedPageLockTracker;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.SharedPageLockTrackerDump;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class ToFileDumpProcessorTest {
    /** */
    private Path file;

    /** */
    @After
    public void afterTest() throws IOException {
        if (file != null)
            Files.delete(file);
    }

    /** */
    @Test
    public void toFileDump() throws Exception {
        Path homeDir = Paths.get(U.defaultWorkDirectory());

        System.out.println("IGNITE_HOME:" + homeDir);

        SharedPageLockTracker pageLockTracker = new SharedPageLockTracker();

        try (PageLockListener tracker = pageLockTracker.registerStructure("dummy")) {
            tracker.onBeforeReadLock(1, 2, 3);

            tracker.onReadLock(1, 2, 3, 4);
        }

        SharedPageLockTrackerDump pageLockDump = pageLockTracker.dump();

        assertNotNull(pageLockDump);

        file = Paths.get(ToFileDumpProcessor.toFileDump(pageLockDump, homeDir, "test"));

        System.out.println("Dump saved:" + file);

        try (Stream<Path> stream = Files.list(homeDir)) {
            assertTrue(stream.map(Path::toAbsolutePath).anyMatch(file::equals));
        }

        String actDumpStr = new String(Files.readAllBytes(file), StandardCharsets.UTF_8);

        assertEquals(ToStringDumpHelper.toStringDump(pageLockDump), actDumpStr);
    }
}
