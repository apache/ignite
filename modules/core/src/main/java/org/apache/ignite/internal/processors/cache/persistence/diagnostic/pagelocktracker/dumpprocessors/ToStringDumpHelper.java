/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.PageLockDump;
import org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker.SharedPageLockTrackerDump;

/**
 * Helper for creating string from {@link PageLockDump}.
 */
public class ToStringDumpHelper {
    /** Date format. */
    public static final DateTimeFormatter DATE_FMT = DateTimeFormatter
        .ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
        .withZone(ZoneId.systemDefault());

    /**
     * @param pageLockDump Dump.
     * @return String representation of dump.
     */
    public static String toStringDump(PageLockDump pageLockDump) {
        StringBuilder sb = new StringBuilder();

        ToStringDumpProcessor proc = new ToStringDumpProcessor(sb, String::valueOf);

        proc.processDump(pageLockDump);

        return sb.toString();
    }

    /**
     * @param pageLockDump Dump.
     * @return String representation of dump.
     */
    public static String toStringDump(SharedPageLockTrackerDump pageLockDump) {
        StringBuilder sb = new StringBuilder();

        ToStringDumpProcessor proc = new ToStringDumpProcessor(sb, pageLockDump.structureIdToStructureName::get);

        proc.processDump(pageLockDump);

        return sb.toString();
    }
}
