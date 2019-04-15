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

package org.apache.ignite.testsuites;

import java.util.List;
import org.apache.ignite.internal.processors.cache.IgniteCacheConfigVariationsQueryTest;
import org.apache.ignite.testframework.configvariations.ConfigVariationsTestSuiteBuilder;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/**
 * Test suite for cache queries.
 */
@RunWith(DynamicSuite.class)
public class IgniteCacheConfigVariationQueryTestSuite {
    /** */
    public static List<Class<?>> suite() {
        return new ConfigVariationsTestSuiteBuilder(IgniteCacheConfigVariationsQueryTest.class)
            .withBasicCacheParams()
            .gridsCount(5).backups(1)
            .testedNodesCount(3).withClients()
            .classes();
    }
}
