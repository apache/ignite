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

/**
 * CDC consumer that log all records.
 */
public class LogAllCDCConsumer implements CDCConsumer {
    /** Ignite logger. */
    private IgniteLogger log;

    /** {@inheritDoc} */
    @Override public void start(IgniteConfiguration configuration) {
        log = configuration.getGridLogger().getLogger(LogAllCDCConsumer.class);
    }

    /** {@inheritDoc} */
    @Override public <T extends WALRecord> void onRecord(T record) {
        log.info(Objects.toString(record));
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        // No-op.
    }
}