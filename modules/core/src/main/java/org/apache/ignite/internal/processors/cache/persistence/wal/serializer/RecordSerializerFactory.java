/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.persistence.wal.serializer;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.FilteredRecord;
import org.apache.ignite.internal.pagemem.wal.record.MarshalledRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.lang.IgniteBiPredicate;

/**
 * Factory for creating {@link RecordSerializer}.
 */
public interface RecordSerializerFactory {
    /** Latest serializer version to use. */
    static final int LATEST_SERIALIZER_VERSION = 2;
    /**
     * Factory method for creation {@link RecordSerializer}.
     *
     * @param ver Serializer version.
     * @return record serializer.
     */
    public RecordSerializer createSerializer(int ver) throws IgniteCheckedException;

    /**
     * TODO: This flag was added under IGNITE-6029, but still unused. Should be either handled or removed.
     *
     * @param writePointer Write pointer flag.
     */
    public RecordSerializerFactory writePointer(boolean writePointer);

    /**
     * Specifies deserialization filter. Created serializer will read bulk {@link FilteredRecord} instead of actual
     * record if record type/pointer doesn't satisfy filter.
     *
     * @param readTypeFilter Read type filter.
     */
    public RecordSerializerFactory recordDeserializeFilter(IgniteBiPredicate<WALRecord.RecordType, WALPointer> readTypeFilter);

    /**
     * If marshalledMode is on, created serializer will read {@link MarshalledRecord} with raw binary data instead of
     * actual record.
     * Useful for copying binary data from WAL.
     *
     * @param marshalledMode Marshalled mode.
     */
    public RecordSerializerFactory marshalledMode(boolean marshalledMode);

    /**
     * If skipPositionCheck is true, created serializer won't check that actual position of record in file is equal to
     * position in saved record's WALPointer.
     * Must be true if we are reading from compacted WAL segment.
     *
     * @param skipPositionCheck Skip position check.
     */
    public RecordSerializerFactory skipPositionCheck(boolean skipPositionCheck);
}