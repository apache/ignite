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
import junit.framework.TestCase;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.testframework.config.params.Variants;

/**
 * Test.
 */
public class VariantsTest extends TestCase {
    /**
     * @throws Exception If failed.
     */
    public void test() throws Exception {
        IgniteClosure<CacheConfiguration, Void>[] modes =
            Variants.enumVariants(CacheMode.class, "setCacheMode");

        assertEquals(CacheMode.values().length, modes.length);

        Set<CacheMode> res = new HashSet<>();

        for (IgniteClosure<CacheConfiguration, Void> modeApplier : modes) {
            CacheConfiguration cfg = new CacheConfiguration();

            modeApplier.apply(cfg);

            CacheMode mode = cfg.getCacheMode();

            res.add(mode);

            System.out.println(">>> " + mode);
        }

        assertEquals(modes.length, res.size());
    }
}
