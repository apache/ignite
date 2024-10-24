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

package org.apache.ignite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.cache.Cache;
import org.apache.ignite.cache.ApplicationContext;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheInterceptor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** */
@RunWith(Parameterized.class)
public class ApplicationContextTest extends GridCommonAbstractTest {
    /** */
    @Parameterized.Parameter
    public CacheAtomicityMode mode;

    /** */
    @Parameterized.Parameter(1)
    public boolean cln;

    /** */
    @Parameterized.Parameters(name = "mode={0} cln={1}")
    public static List<Object[]> params() {
        List<Object[]> params = new ArrayList<>();

        for (CacheAtomicityMode mode: CacheAtomicityMode.values()) {
            for (boolean cln : new boolean[] {true, false})
                params.add(new Object[] {mode, cln});
        }

        return params;
    }

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(new CacheConfiguration<String, String>(DEFAULT_CACHE_NAME)
                .setAtomicityMode(mode)
                .setInterceptor(new ApplicationContextInterceptor())
            );
    }

    /** */
    @Override protected void afterTest() {
        stopAllGrids();
    }

    /** */
    @Test
    public void testCacheInterceptor() throws Exception {
        Ignite ign = startGrids(2);

        if (cln)
            ign = startClientGrid(2);

        Map<String, String> attrs = new HashMap<>();

        for (int i = 0; i < 10; i++)
            attrs.put("key" + i, String.valueOf(i));

        IgniteCache<String, String> cache = ign.cache(DEFAULT_CACHE_NAME);
        cache = cache.withApplicationAttributes(attrs);

        for (int i = 0; i < 11; i++)
            cache.put("key" + i, "val");

        for (int i = 0; i < 10; i++)
            assertEquals(String.valueOf(i), cache.get("key" + i));

        assertNull(cache.get("key10"));
    }

    /** */
    public static class ApplicationContextInterceptor implements CacheInterceptor<String, String> {
        /** */
        @Override public @Nullable String onGet(String key, @Nullable String val) {
            return val;
        }

        /** */
        @Override public @Nullable String onBeforePut(Cache.Entry<String, String> entry, String newVal) {
            if (ApplicationContext.getAttributes() == null)
                System.out.println();

            return ApplicationContext.getAttributes().get(entry.getKey());
        }

        /** */
        @Override public void onAfterPut(Cache.Entry<String, String> entry) {

        }

        /** */
        @Override public @Nullable IgniteBiTuple<Boolean, String> onBeforeRemove(Cache.Entry<String, String> entry) {
            return null;
        }

        /** */
        @Override public void onAfterRemove(Cache.Entry<String, String> entry) {

        }
    }
}
