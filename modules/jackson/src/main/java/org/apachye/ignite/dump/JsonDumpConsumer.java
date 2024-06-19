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

package org.apachye.ignite.dump;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cdc.TypeMapping;
import org.apache.ignite.dump.DumpConsumer;
import org.apache.ignite.dump.DumpEntry;
import org.apache.ignite.internal.jackson.IgniteObjectMapper;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.util.typedef.internal.CU;

/**
 *
 */
public class JsonDumpConsumer implements DumpConsumer {
    /** */
    private ObjectMapper mapper;

    /** */
    private PrintStream out = System.out;

    /** {@inheritDoc} */
    @Override public void start() {
        mapper = new IgniteObjectMapper(null); //TODO: FIXME.

    }

    /** {@inheritDoc} */
    @Override public void onMappings(Iterator<TypeMapping> mappings) {

    }

    /** {@inheritDoc} */
    @Override public void onTypes(Iterator<BinaryType> types) {

    }

    /** {@inheritDoc} */
    @Override public void onCacheConfigs(Iterator<StoredCacheData> caches) {

    }

    /** {@inheritDoc} */
    @Override public void onPartition(int grp, int part, Iterator<DumpEntry> data) {
        data.forEachRemaining(e -> {
            Map<String, Object> map = new LinkedHashMap<>();

            map.put("key", e.key());
            map.put("value", e.value());
            map.put("cacheId", e.cacheId());
            map.put("version", e.version());
            if (e.expireTime() != CU.EXPIRE_TIME_ETERNAL)
                map.put("expireTime", e.expireTime());

            try {
                mapper.writeValue(out, map);
            }
            catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void stop() {

    }
}
