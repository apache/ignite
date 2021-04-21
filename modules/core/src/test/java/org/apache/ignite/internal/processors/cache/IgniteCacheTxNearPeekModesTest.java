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

import org.apache.ignite.testframework.MvccFeatureChecker;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests peek modes with near tx cache.
 */
public class IgniteCacheTxNearPeekModesTest extends IgniteCacheTxPeekModesTest {
    /** */
    @Before
    public void beforeIgniteCacheTxNearPeekModesTest() {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.NEAR_CACHE);
    }

    /** {@inheritDoc} */
    @Override protected boolean hasNearCache() {
        return true;
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testLocalPeek() throws Exception {
        // TODO: uncomment and re-open ticket if fails.
//        fail("https://issues.apache.org/jira/browse/IGNITE-1824");

        super.testLocalPeek();
    }
}
