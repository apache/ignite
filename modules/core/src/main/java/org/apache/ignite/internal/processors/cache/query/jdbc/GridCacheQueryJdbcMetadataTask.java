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

package org.apache.ignite.internal.processors.cache.query.jdbc;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.managers.discovery.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.marshaller.jdk.*;
import org.apache.ignite.resources.*;
import org.jetbrains.annotations.*;

import java.sql.*;
import java.util.*;

/**
 * Task that gets metadata for JDBC adapter.
 */
public class GridCacheQueryJdbcMetadataTask extends ComputeTaskAdapter<String, byte[]> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Marshaller. */
    private static final Marshaller MARSHALLER = new JdkMarshaller();

    /** */
    @IgniteInstanceResource
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable String cacheName) {
        Map<JdbcDriverMetadataJob, ClusterNode> map = new HashMap<>();

        IgniteKernal kernal = (IgniteKernal)ignite;

        GridDiscoveryManager discoMgr = kernal.context().discovery();

        for (ClusterNode n : subgrid)
            if (discoMgr.cacheAffinityNode(n, cacheName)) {
                map.put(new JdbcDriverMetadataJob(cacheName), n);

                break;
            }

        return map;
    }

    /** {@inheritDoc} */
    @Override public byte[] reduce(List<ComputeJobResult> results) {
        return F.first(results).getData();
    }

    /**
     * Job for JDBC adapter.
     */
    private static class JdbcDriverMetadataJob extends ComputeJobAdapter {
        /** */
        private static final long serialVersionUID = 0L;

        /** Cache name. */
        private final String cacheName;

        /** Grid instance. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** Logger. */
        @LoggerResource
        private IgniteLogger log;

        /**
         * @param cacheName Cache name.
         */
        private JdbcDriverMetadataJob(@Nullable String cacheName) {
            this.cacheName = cacheName;
        }

        /** {@inheritDoc} */
        @Override public Object execute() {
            byte status;
            byte[] data;

            try {
                IgniteInternalCache<?, ?> cache = ((IgniteEx) ignite).cachex(cacheName);

                assert cache != null;

                Collection<GridCacheSqlMetadata> metas = cache.context().queries().sqlMetadata();

                Map<String, Map<String, Map<String, String>>> schemasMap = U.newHashMap(metas.size());

                Collection<List<Object>> indexesInfo = new LinkedList<>();

                for (GridCacheSqlMetadata meta : metas) {
                    String name = meta.cacheName();

                    if (name == null)
                        name = "PUBLIC";

                    Collection<String> types = meta.types();

                    Map<String, Map<String, String>> typesMap = U.newHashMap(types.size());

                    for (String type : types) {
                        typesMap.put(type.toUpperCase(), meta.fields(type));

                        for (GridCacheSqlIndexMetadata idx : meta.indexes(type)) {
                            int cnt = 0;

                            for (String field : idx.fields()) {
                                indexesInfo.add(F.<Object>asList(name, type.toUpperCase(), !idx.unique(),
                                    idx.name().toUpperCase(), ++cnt, field, idx.descending(field)));
                            }
                        }
                    }

                    schemasMap.put(name, typesMap);
                }

                status = 0;

                data = MARSHALLER.marshal(F.asList(schemasMap, indexesInfo));
            }
            catch (Throwable t) {
                U.error(log, "Failed to get metadata for JDBC.", t);

                SQLException err = new SQLException(t.getMessage());

                status = 1;

                try {
                    data = MARSHALLER.marshal(err);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }

                if (t instanceof Error)
                    throw (Error)t;
            }

            byte[] packet = new byte[data.length + 1];

            packet[0] = status;

            U.arrayCopy(data, 0, packet, 1, data.length);

            return packet;
        }
    }
}
