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

package org.apache.ignite.testframework.test;

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.configvariations.ConfigParameter;
import org.apache.ignite.testframework.configvariations.Parameters;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test.
 */
public class ParametersTest {
    /** */
    private static final String DEFAULT_CACHE_NAME = "default";

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEnumVariations() throws Exception {
        ConfigParameter<CacheConfiguration>[] modes = Parameters.enumParameters("setCacheMode", CacheMode.class);

        assertEquals(CacheMode.values().length, modes.length);

        Set<CacheMode> res = new HashSet<>();

        for (ConfigParameter<CacheConfiguration> modeApplier : modes) {
            CacheConfiguration cfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

            modeApplier.apply(cfg);

            CacheMode mode = cfg.getCacheMode();

            res.add(mode);

            System.out.println(">>> " + mode);
        }

        assertEquals(modes.length, res.size());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEnumVariationsWithNull() throws Exception {
        ConfigParameter<CacheConfiguration>[] cfgParam =
            Parameters.enumParameters(true, "setCacheMode", CacheMode.class);

        assertEquals(CacheMode.values().length + 1, cfgParam.length);

        cfgParam[0] = null;

        Set<CacheMode> set = new HashSet<>();

        for (int i = 1; i < cfgParam.length; i++) {
            ConfigParameter<CacheConfiguration> modeApplier = cfgParam[i];

            CacheConfiguration cfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

            modeApplier.apply(cfg);

            CacheMode mode = cfg.getCacheMode();

            set.add(mode);

            System.out.println(">>> " + mode);
        }

        assertEquals(CacheMode.values().length, set.size());
    }
}
