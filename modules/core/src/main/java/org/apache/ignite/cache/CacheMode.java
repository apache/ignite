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

package org.apache.ignite.cache;

import java.util.stream.Stream;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.jetbrains.annotations.Nullable;

/**
 * Enumeration of all supported caching modes. Cache mode is specified in {@link org.apache.ignite.configuration.CacheConfiguration}
 * and cannot be changed after cache has started.
 */
public enum CacheMode {
    /**
     * Specifies fully replicated cache behavior. In this mode all the keys are distributed
     * to all participating nodes. User still has affinity control
     * over subset of nodes for any given key via {@link AffinityFunction}
     * configuration.
     */
    REPLICATED((byte)1),

    /**
     * Specifies partitioned cache behaviour. In this mode the overall
     * key set will be divided into partitions and all partitions will be split
     * equally between participating nodes. User has affinity
     * control over key assignment via {@link AffinityFunction}
     * configuration.
     * <p>
     * Note that partitioned cache is always fronted by local
     * {@code 'near'} cache which stores most recent data. You
     * can configure the size of near cache via {@link NearCacheConfiguration#getNearEvictionPolicyFactory()}
     * configuration property.
     */
    PARTITIONED((byte)2);

    /** Cached enumerated values by their codes. */
    private static final CacheMode[] BY_CODE;

    static {
        int max = Stream.of(values())
            .mapToInt(e -> e.code)
            .max()
            .orElseThrow(RuntimeException::new);

        BY_CODE = new CacheMode[max + 1];

        for (CacheMode e : values()) {
            BY_CODE[e.code] = e;
        }
    }

    /** Cache mode code. */
    private final byte code;

    /**
     * @param code Cache mode code.
     */
    CacheMode(byte code) {
        this.code = code;
    }

    /**
     * @return Cache mode code.
     */
    public byte code() {
        return code;
    }

    /**
     * Efficiently gets enumerated value from its code.
     *
     * @param code Code.
     * @return Enumerated value or {@code null} if an out of range.
     */
    @Nullable public static CacheMode fromCode(int code) {
        return code >= 0 && code < BY_CODE.length ? BY_CODE[code] : null;
    }

    /**
     * @param mode Cache mode.
     * @return Cache mode code.
     */
    public static byte toCode(@Nullable CacheMode mode) {
        return mode == null ? -1 : mode.code;
    }
}
