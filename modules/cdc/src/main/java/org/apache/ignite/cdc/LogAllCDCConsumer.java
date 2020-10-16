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

package org.apache.ignite.cdc;

import java.util.Objects;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;

import static org.apache.ignite.internal.pagemem.wal.record.WALRecord.RecordPurpose.LOGICAL;

/**
 * CDC consumer that log all records.
 */
public class LogAllCDCConsumer implements CDCConsumer {
    /** Ignite logger. */
    private IgniteLogger log;

    @Override public String id() {
        return "log-all-consumer";
    }

    /** {@inheritDoc} */
    @Override public void start(IgniteConfiguration configuration, IgniteLogger log) {
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public <T extends WALRecord> void onRecord(T record) {
        if (record.type().purpose() == LOGICAL)
            log.info(Objects.toString(record));
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        // No-op.
    }

    @Override public String toString() {
        return "LogAllCDCConsumer{id=" + id() + '}';
    }
}