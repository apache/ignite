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
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.SharedPageLockTrackerDump;

import static org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.dumpprocessors.ToStringDumpHelper.toStringDump;

/**
 *
 */
public class ToFileDumpProcessor {
    /** Date format. */
    public static final DateTimeFormatter DATE_FMT = DateTimeFormatter
        .ofPattern("yyyy_MM_dd_HH_mm_ss_SSS")
        .withZone(ZoneId.systemDefault());

    /** File name prefix. */
    public static final String PREFIX_NAME = "page_lock_dump_";

    /**
     * @param pageLockDump Dump.
     * @param dir Directory to save.
     */
    public static String toFileDump(SharedPageLockTrackerDump pageLockDump, Path dir, String name) throws IgniteCheckedException {
        try {
            Files.createDirectories(dir);

            String dumpName = PREFIX_NAME + name + "_" + DATE_FMT.format(Instant.ofEpochMilli(pageLockDump.time));

            Path file = dir.resolve(dumpName);

            Files.write(file, toStringDump(pageLockDump).getBytes(StandardCharsets.UTF_8));

            return file.toAbsolutePath().toString();
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
    }
}
