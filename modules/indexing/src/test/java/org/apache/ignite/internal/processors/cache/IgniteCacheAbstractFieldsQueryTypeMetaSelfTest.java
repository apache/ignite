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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.configuration.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.apache.ignite.cache.CacheMode.*;

/**
 * Tests for fields queries configured via {@link CacheTypeMetadata}.
 */
public abstract class IgniteCacheAbstractFieldsQueryTypeMetaSelfTest extends IgniteCacheAbstractFieldsQuerySelfTest {
    /**
     * @param name Cache name.
     * @param primitives Index primitives.
     * @return Cache.
     */
    @Override protected CacheConfiguration cache(@Nullable String name, boolean primitives) {
        CacheConfiguration cache = defaultCacheConfiguration();

        cache.setName(name);
        cache.setCacheMode(cacheMode());
        cache.setAtomicityMode(atomicityMode());
        cache.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cache.setRebalanceMode(CacheRebalanceMode.SYNC);

        Collection<CacheTypeMetadata> metas;

        if (EMPTY_CACHE.equals(name))
            metas = new ArrayList<>();
        else {
            metas = new ArrayList<>();

            CacheTypeMetadata meta = new CacheTypeMetadata();

            meta.setKeyType(String.class);
            meta.setValueType(Organization.class);
            Map<String, Class<?>> qryFlds = meta.getQueryFields();
            qryFlds.put("id", int.class);
            qryFlds.put("name", String.class);

            metas.add(meta);

            if (CACHE_COMPLEX_KEYS.equals(name)) {
                meta = new CacheTypeMetadata();
                meta.setKeyType(PersonKey.class);
                meta.setValueType(Person.class);

                qryFlds = meta.getQueryFields();
                qryFlds.put("id", UUID.class);
                qryFlds.put("name", String.class);
                qryFlds.put("age", int.class);
                qryFlds.put("orgId", int.class);

                Map<String, Class<?>> ascFlds = meta.getAscendingFields();
                ascFlds.put("age", int.class);
                ascFlds.put("orgId", int.class);

                metas.add(meta);
            }
            else {
                meta = new CacheTypeMetadata();
                meta.setKeyType(AffinityKey.class);
                meta.setValueType(Person.class);

                qryFlds = meta.getQueryFields();
                qryFlds.put("name", String.class);
                qryFlds.put("age", int.class);
                qryFlds.put("orgId", int.class);

                Map<String, Class<?>> ascFlds = meta.getAscendingFields();
                ascFlds.put("age", int.class);
                ascFlds.put("orgId", int.class);

                metas.add(meta);
            }

            if (!CACHE_NO_PRIMITIVES.equals(name)) {
                meta = new CacheTypeMetadata();
                meta.setKeyType(String.class);
                meta.setValueType(String.class);
                metas.add(meta);

                meta = new CacheTypeMetadata();
                meta.setKeyType(Integer.class);
                meta.setValueType(Integer.class);
                metas.add(meta);
            }
        }

        cache.setTypeMetadata(metas);

        if (cacheMode() == PARTITIONED)
            cache.setBackups(1);

        return cache;
    }
}
