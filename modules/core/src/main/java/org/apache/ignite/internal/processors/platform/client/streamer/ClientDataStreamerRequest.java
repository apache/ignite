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

package org.apache.ignite.internal.processors.platform.client.streamer;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerEntry;
import org.apache.ignite.internal.processors.platform.client.ClientRequest;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.client.ClientStatus;
import org.apache.ignite.internal.util.typedef.T2;

import static org.apache.ignite.internal.processors.platform.utils.PlatformUtils.ObjectWithBytes;
import static org.apache.ignite.internal.processors.platform.utils.PlatformUtils.buildCacheObject;
import static org.apache.ignite.internal.processors.platform.utils.PlatformUtils.readCacheObject;

/**
 * Base class for streamer requests.
 */
abstract class ClientDataStreamerRequest extends ClientRequest {
    /**
     * Constructor.
     *
     * @param reader Data reader.
     */
    protected ClientDataStreamerRequest(BinaryRawReader reader) {
        super(reader);
    }

    /**
     * Returns invalid node state response.
     *
     * @return Invalid node state response.
     */
    protected ClientResponse getInvalidNodeStateResponse() {
        return new ClientResponse(requestId(), ClientStatus.INVALID_NODE_STATE,
                "Data streamer has been closed because node is stopping.");
    }

    /**
     * Reads entries.
     *
     * @param reader Data reader.
     * @return Streamer entries.
     */
    protected static Collection<T2<ObjectWithBytes, ObjectWithBytes>> read(BinaryReaderExImpl reader) {
        int entriesCnt = reader.readInt();

        if (entriesCnt == 0)
            return null;

        Collection<T2<ObjectWithBytes, ObjectWithBytes>> entries = new ArrayList<>(entriesCnt);

        for (int i = 0; i < entriesCnt; i++)
            entries.add(new T2<>(readCacheObject(reader), readCacheObject(reader)));

        return entries;
    }

    /**
     * Builds entries.
     *
     * @param cotx Context.
     * @param entries Entries.
     * @return Streamer entries.
     */
    protected static Collection<DataStreamerEntry> build(CacheObjectValueContext cotx,
        Collection<T2<ObjectWithBytes, ObjectWithBytes>> entries) {
        Collection<DataStreamerEntry> dsEntries = entries != null ? new ArrayList<>(entries.size()) : null;

        for (T2<ObjectWithBytes, ObjectWithBytes> t2 : entries)
            dsEntries.add(new DataStreamerEntry(
                buildCacheObject(cotx, t2.getKey(), true),
                buildCacheObject(cotx, t2.getValue(), false)));

        return dsEntries;
    }
}
