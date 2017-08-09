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

package org.apache.ignite.internal.pagemem.wal.record;

import org.apache.ignite.internal.pagemem.wal.WALPointer;

/**
 * Interface is needed to link WAL reference of {@link DataRecord} entries
 * to appropriate physical DataPage insert/update records.
 */
public interface WALReferenceAwareRecord {
    /**
     * Record payload size which is needed for extracting actual payload from WAL reference.
     *
     * @return Payload size in bytes.
     */
    int payloadSize();

    /**
     * In case of fragmented record - offset of fragmented payload relatively to whole payload.
     *
     * @return Offset in bytes or -1 if record is not fragmented.
     */
    int offset();

    /**
     * Set payload extracted from {@link DataRecord} to this record.
     *
     * @param payload Payload.
     */
    void payload(byte[] payload);

    /**
     * WAL reference to appropriate {@link DataRecord}.
     *
     * @return WAL reference.
     */
    WALPointer reference();
}
