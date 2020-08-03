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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockDump;

import static java.nio.channels.FileChannel.open;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 *
 */
public class ToFileDumpProcessor {
    /** Date format. */
    public static final SimpleDateFormat DATE_FMT = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss_SSS");

    /** File name prefix. */
    public static final String PREFIX_NAME = "page_lock_dump_";

    /**
     * @param pageLockDump Dump.
     * @param dir Directory to save.
     */
    public static String toFileDump(PageLockDump pageLockDump, File dir, String name) throws IgniteCheckedException {
        try {
           if (!dir.exists())
               dir.mkdirs();

            File file = new File(dir, PREFIX_NAME + name + "_" + DATE_FMT.format(new Date(pageLockDump.time())));

            return saveToFile(ToStringDumpProcessor.toStringDump(pageLockDump), file);
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * @param dump Dump.
     * @param file File to save.
     */
    private static String saveToFile(String dump, File file) throws IOException {
        assert dump != null;
        assert file != null;
        assert !dump.isEmpty();

        try (FileChannel ch = open(file.toPath(), CREATE_NEW, WRITE)) {
            ByteBuffer buf = ByteBuffer.wrap(dump.getBytes());

            assert buf.position() == 0;
            assert buf.limit() > 0;

            while (buf.position() != buf.limit())
                ch.write(buf);

            ch.force(true);
        }

        return file.getAbsolutePath();
    }
}
