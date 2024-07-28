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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.ignite.cache.CacheEntryVersion;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.DumpConsumerKernalContextAware;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.IgniteCacheDumpSelfTest;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.platform.model.ACL;
import org.apache.ignite.platform.model.AccessLevel;
import org.apache.ignite.platform.model.Key;
import org.apache.ignite.platform.model.Role;
import org.apache.ignite.platform.model.User;
import org.apache.ignite.platform.model.Value;
import org.jetbrains.annotations.NotNull;

/** */
public class JsonDumpConsumerTest extends IgniteCacheDumpSelfTest {
    /** {@inheritDoc} */
    @Override protected TestDumpConsumer dumpConsumer(
        Set<String> expectedFoundCaches,
        int expectedDfltDumpSz,
        int expectedGrpDumpSz,
        int expectedCnt
    ) {
        return new TestJsonDumpConsumer(expectedFoundCaches, expectedDfltDumpSz, expectedGrpDumpSz, expectedCnt);
    }

    /** */
    public class TestJsonDumpConsumer extends TestDumpConsumerImpl implements DumpConsumerKernalContextAware {
        /** */
        private final JsonDumpConsumer jsonDumpConsumer = new JsonDumpConsumer();

        /** */
        protected TestJsonDumpConsumer(Set<String> expectedFoundCaches, int expectedDfltDumpSz, int expectedGrpDumpSz, int expectedCnt) {
            super(expectedFoundCaches, expectedDfltDumpSz, expectedGrpDumpSz, expectedCnt);
        }

        /** {@inheritDoc} */
        @Override public void start(GridKernalContext ctx) {
            jsonDumpConsumer.start(ctx);
            start();
        }

        /** {@inheritDoc} */
        @Override public void onPartition(int grp, int part, Iterator<DumpEntry> data) {
            ByteArrayOutputStream testOut = new ByteArrayOutputStream((int)(16 * U.MB));

            PrintStream out = System.out;

            System.setOut(new PrintStream(testOut));

            // Parse entries from System.out.
            Scanner sc;

            try {
                // Print out all entries to System.out.
                jsonDumpConsumer.onPartition(grp, part, data);

                sc = new Scanner(testOut.toString());
            }
            finally {
                System.setOut(out);
            }

            TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {
                // No-op.
            };

            ObjectMapper mapper = new ObjectMapper();

            super.onPartition(grp, part, new Iterator<DumpEntry>() {
                @Override public boolean hasNext() {
                    return sc.hasNextLine();
                }

                @Override public DumpEntry next() {
                    try {
                        Map<String, Object> entryFromJson = mapper.readValue(sc.nextLine(), typeRef);

                        return new DumpEntry() {
                            @Override public int cacheId() {
                                return Integer.parseInt(entryFromJson.get("cacheId").toString());
                            }

                            @Override public long expireTime() {
                                return Long.parseLong(entryFromJson.get("expireTime").toString());
                            }

                            @Override public CacheEntryVersion version() {
                                return JsonDumpConsumerTest.version((Map<String, Object>)entryFromJson.get("version"));
                            }

                            @Override public Object key() {
                                if (cacheId() == CU.cacheId(DEFAULT_CACHE_NAME) || cacheId() == CU.cacheId(CACHE_0))
                                    return Integer.parseInt(entryFromJson.get("key").toString());

                                Map<String, Object> key = (Map<String, Object>)entryFromJson.get("key");

                                return new Key(Long.parseLong(key.get("id").toString()));
                            }

                            @Override public Object value() {
                                if (cacheId() == CU.cacheId(DEFAULT_CACHE_NAME))
                                    return Integer.valueOf(entryFromJson.get("value").toString());
                                else if (cacheId() == CU.cacheId(CACHE_0)) {
                                    Map<String, Object> val = (Map<String, Object>)entryFromJson.get("value");
                                    Map<String, Object> role = (Map<String, Object>)val.get("role");

                                    return new User(
                                        (Integer)val.get("id"),
                                        ACL.valueOf(val.get("acl").toString()),
                                        new Role(
                                            role.get("name").toString(),
                                            AccessLevel.valueOf(role.get("accessLevel").toString())
                                        )
                                    );
                                }
                                else if (cacheId() == CU.cacheId(CACHE_1)) {
                                    Map<String, Object> val = (Map<String, Object>)entryFromJson.get("value");

                                    return new Value(val.get("val").toString());
                                }

                                throw new IllegalArgumentException("Unknown cache: " + cacheId());
                            }
                        };
                    }
                    catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
    }

    /** */
    private static CacheEntryVersion version(Map<String, Object> version) {
        return new CacheEntryVersion() {
            @Override public long order() {
                return Long.parseLong(version.get("order").toString());
            }

            @Override public int nodeOrder() {
                return Integer.parseInt(version.get("nodeOrder").toString());
            }

            @Override public byte clusterId() {
                return Byte.parseByte(version.get("clusterId").toString());
            }

            @Override public int topologyVersion() {
                return Integer.parseInt(version.get("topologyVersion").toString());
            }

            @Override public CacheEntryVersion otherClusterVersion() {
                return version.containsKey("otherClusterVersion")
                        ? version((Map<String, Object>)version.get("otherClusterVersion"))
                        : null;
            }

            @Override public int compareTo(@NotNull CacheEntryVersion o) {
                throw new UnsupportedOperationException();
            }
        };
    }
}
