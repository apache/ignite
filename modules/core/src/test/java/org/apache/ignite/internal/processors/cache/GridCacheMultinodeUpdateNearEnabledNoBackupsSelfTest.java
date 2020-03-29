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

import org.apache.ignite.configuration.CacheConfiguration;
import org.junit.Assume;
import org.junit.Test;
import org.apache.ignite.testframework.MvccFeatureChecker;

/**
 *
 */
public class GridCacheMultinodeUpdateNearEnabledNoBackupsSelfTest extends GridCacheMultinodeUpdateNearEnabledSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(igniteInstanceName);

        ccfg.setBackups(0);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testInvoke() throws Exception {
        Assume.assumeTrue("https://issues.apache.org/jira/browse/IGNITE-809", MvccFeatureChecker.forcedMvcc());

        super.testInvoke();
    }
}
