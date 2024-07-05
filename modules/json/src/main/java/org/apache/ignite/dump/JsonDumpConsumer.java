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

package org.apache.ignite.dump;

import java.io.IOException;
import java.util.Iterator;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.CacheEntryVersion;
import org.apache.ignite.cdc.TypeMapping;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.jackson.IgniteObjectMapper;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.DumpConsumerKernalContextAware;

/**
 * Dump consumer that outputs entries in json format.
 */
public class JsonDumpConsumer implements DumpConsumerKernalContextAware {
    /** Ignite specific object mapper. */
    private IgniteObjectMapper mapper;

    /** {@inheritDoc} */
    @Override public void start(GridKernalContext ctx) {
        mapper = new IgniteObjectMapper(ctx);
    }

    /** {@inheritDoc} */
    @Override public void onMappings(Iterator<TypeMapping> mappings) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onTypes(Iterator<BinaryType> types) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onCacheConfigs(Iterator<StoredCacheData> caches) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onPartition(int grp, int part, Iterator<DumpEntry> data) {
        data.forEachRemaining(entry -> {
            try {
                System.out.println(mapper.writeValueAsString(new PrintableDumpEntry(entry)));
            }
            catch (IOException e) {
                throw new IgniteException(e);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void start() {
        // No-op.
    }

    /** */
    private static class PrintableDumpEntry {
        /** */
        private final DumpEntry e;

        /** */
        public PrintableDumpEntry(DumpEntry e) {
            this.e = e;
        }

        /** @see DumpEntry#cacheId() */
        public int getCacheId() {
            return e.cacheId();
        }

        /** @see DumpEntry#expireTime() */
        public long getExpireTime() {
            return e.expireTime();
        }

        /** @see DumpEntry#version() */
        public PrintableCacheEntryVersion getVersion() {
            return new PrintableCacheEntryVersion(e.version());
        }

        /** @see DumpEntry#key() */
        public Object getKey() {
            return e.key();
        }

        /** @see DumpEntry#value() */
        public Object getValue() {
            return e.value();
        }
    }

    /** */
    private static class PrintableCacheEntryVersion {
        /** */
        private final CacheEntryVersion v;

        /** */
        public PrintableCacheEntryVersion(CacheEntryVersion v) {
            this.v = v;
        }

        /** @see CacheEntryVersion#order() */
        public long getOrder() {
            return v.order();
        }

        /** @see CacheEntryVersion#nodeOrder() */
        public int getNodeOrder() {
            return v.nodeOrder();
        }

        /** @see CacheEntryVersion#clusterId() */
        public byte getClusterId() {
            return v.clusterId();
        }

        /** @see CacheEntryVersion#topologyVersion() */
        public int getTopologyVersion() {
            return v.topologyVersion();
        }

        /** @see CacheEntryVersion#otherClusterVersion() */
        public PrintableCacheEntryVersion otherClusterVersion() {
            return new PrintableCacheEntryVersion(v.otherClusterVersion());
        }
    }
}
