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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.client.thin.TcpClientCache;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientResponse;
import org.apache.ignite.internal.processors.platform.client.tx.ClientTxAwareRequest;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.platform.utils.PlatformUtils.ObjectWithBytes;
import static org.apache.ignite.internal.processors.platform.utils.PlatformUtils.buildCacheObject;
import static org.apache.ignite.internal.processors.platform.utils.PlatformUtils.readCacheObject;

/**
 * Client {@link TcpClientCache#removeAllConflict(Map)} request.
 */
public class ClientCacheRemoveAllConflictRequest extends ClientCacheDataRequest implements ClientTxAwareRequest {
    /** */
    private final Collection<T2<ObjectWithBytes, GridCacheVersion>> entries;

    /**
     * Constructor.
     *
     * @param reader Reader.
     */
    public ClientCacheRemoveAllConflictRequest(BinaryReaderExImpl reader) {
        super(reader);

        int cnt = reader.readInt();

        entries = new ArrayList<>(cnt);

        for (int i = 0; i < cnt; i++) {
            ObjectWithBytes key = readCacheObject(reader);
            GridCacheVersion ver = (GridCacheVersion)reader.readObjectDetached();

            entries.add(new T2<>(key, ver));
        }
    }

    /** {@inheritDoc} */
    @Override public ClientResponse process(ClientConnectionContext ctx) {
        try {
            CacheObjectValueContext cotx = cacheObjectContext(ctx);

            Map<KeyCacheObject, GridCacheVersion> map = new LinkedHashMap<>(entries.size());

            for (T2<ObjectWithBytes, GridCacheVersion> t2 : entries) {
                KeyCacheObject key = buildCacheObject(cotx, t2.get1(), true);
                GridCacheVersion ver = t2.get2();

                map.put(key, ver);
            }

            cachex(ctx).removeAllConflict(map);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }

        return super.process(ctx);
    }
}
