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

package org.apache.ignite.internal.processors.platform.client.cache;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.client.thin.TcpClientCache;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.client.tx.ClientTxAwareRequest;

import static org.apache.ignite.internal.processors.platform.utils.PlatformUtils.readCacheObject;

/**
 * Client {@link TcpClientCache#putAllConflict(Map)} request.
 */
public class ClientCachePutAllConflictRequest extends ClientCacheDataRequest implements ClientTxAwareRequest {
    /** */
    private final Map<KeyCacheObject, GridCacheDrInfo> map;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientCachePutAllConflictRequest(BinaryReaderExImpl reader) {
        super(reader);

        int cnt = reader.readInt();

        map = new LinkedHashMap<>(cnt);

        for (int i = 0; i < cnt; i++) {
            KeyCacheObject key = readCacheObject(reader, true);
            CacheObject val = readCacheObject(reader, false);
            GridCacheVersion ver = (GridCacheVersion)reader.readObjectDetached();

            map.put(key, new GridCacheDrInfo(val, ver));
        }
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        try {
            cachex(ctx).putAllConflict(map);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }

        return super.process(ctx);
    }
}
