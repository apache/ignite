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

package org.gridgain.grid.kernal.processors.cache.query.jdbc;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.marshaller.jdk.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.query.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
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
    private static final IgniteMarshaller MARSHALLER = new IgniteJdkMarshaller();

    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable String cacheName) throws IgniteCheckedException {
        Map<JdbcDriverMetadataJob, ClusterNode> map = new HashMap<>();

        for (ClusterNode n : subgrid)
            if (U.hasCache(n, cacheName)) {
                map.put(new JdbcDriverMetadataJob(cacheName), n);

                break;
            }

        return map;
    }

    /** {@inheritDoc} */
    @Override public byte[] reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
        return F.first(results).getData();
    }

    /**
     * Job for JDBC adapter.
     */
    private static class JdbcDriverMetadataJob extends ComputeJobAdapter implements IgniteOptimizedMarshallable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @SuppressWarnings({"NonConstantFieldWithUpperCaseName", "AbbreviationUsage", "UnusedDeclaration"})
        private static Object GG_CLASS_ID;

        /** Cache name. */
        private final String cacheName;

        /** Grid instance. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** Logger. */
        @IgniteLoggerResource
        private IgniteLogger log;

        /**
         * @param cacheName Cache name.
         */
        private JdbcDriverMetadataJob(@Nullable String cacheName) {
            this.cacheName = cacheName;
        }

        /** {@inheritDoc} */
        @Override public Object ggClassId() {
            return GG_CLASS_ID;
        }

        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteCheckedException {
            byte status;
            byte[] data;

            try {
                GridCache<?, ?> cache = ((GridEx) ignite).cachex(cacheName);

                assert cache != null;

                Collection<GridCacheSqlMetadata> metas = ((GridCacheQueriesEx<?, ?>)cache.queries()).sqlMetadata();

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

                data = MARSHALLER.marshal(err);
            }

            byte[] packet = new byte[data.length + 1];

            packet[0] = status;

            U.arrayCopy(data, 0, packet, 1, data.length);

            return packet;
        }
    }
}
